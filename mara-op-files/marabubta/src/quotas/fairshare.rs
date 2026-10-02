// Marabunta - Licensed under the MIT License.
//! Fair share scheduler integration for the marabunta-compute job placement system
//!
//! This module implements fair share scheduling to ensure equitable resource
//! distribution among accounts based on their historical usage and allocations.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::account::UsageSnapshot;
use super::types::{AccountId, QuotaResource};

/// Configuration for fair share calculation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FairShareConfig {
    /// How far back to look for historical usage
    pub history_window: Duration,
    /// Half-life for exponential decay (older usage matters less)
    pub decay_halflife: Duration,
    /// How to determine target shares
    pub target_share_source: TargetShareSource,
    /// Minimum priority modifier (floor)
    pub min_priority_modifier: i32,
    /// Maximum priority modifier (ceiling)
    pub max_priority_modifier: i32,
}

impl Default for FairShareConfig {
    fn default() -> Self {
        Self {
            history_window: Duration::days(7),
            decay_halflife: Duration::days(1),
            target_share_source: TargetShareSource::Equal,
            min_priority_modifier: -100,
            max_priority_modifier: 100,
        }
    }
}

impl FairShareConfig {
    /// Create a new config with the given history window
    pub fn with_history_window(mut self, window: Duration) -> Self {
        self.history_window = window;
        self
    }

    /// Set the decay half-life
    pub fn with_decay_halflife(mut self, halflife: Duration) -> Self {
        self.decay_halflife = halflife;
        self
    }

    /// Set the target share source
    pub fn with_target_shares(mut self, source: TargetShareSource) -> Self {
        self.target_share_source = source;
        self
    }
}

/// How to determine target fair shares
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TargetShareSource {
    /// Everyone gets equal share
    Equal,
    /// Based on allocation size (larger allocation = larger share)
    Proportional,
    /// Explicit shares defined per account
    Custom(HashMap<AccountId, f64>),
}

/// Fair share calculator for equitable resource distribution
pub struct FairShareCalculator {
    /// Historical usage for fair share calculation (account_id -> snapshots)
    historical_usage: HashMap<AccountId, Vec<UsageSnapshot>>,
    /// Configuration
    config: FairShareConfig,
    /// Cached effective shares (recalculated periodically)
    cached_shares: HashMap<AccountId, f64>,
    /// When shares were last calculated
    shares_calculated_at: Option<DateTime<Utc>>,
}

impl FairShareCalculator {
    /// Create a new fair share calculator
    pub fn new(config: FairShareConfig) -> Self {
        Self {
            historical_usage: HashMap::new(),
            config,
            cached_shares: HashMap::new(),
            shares_calculated_at: None,
        }
    }

    /// Calculate fair share priority modifier for an account
    ///
    /// Returns a priority modifier:
    /// - Positive: account has used less than fair share (boost priority)
    /// - Negative: account has used more than fair share (lower priority)
    /// - Zero: account is at fair share
    pub fn calculate_priority_modifier(
        &self,
        account_id: &AccountId,
        current_usage: &HashMap<QuotaResource, f64>,
    ) -> i32 {
        let target_share = self.get_target_share(account_id);
        let actual_share = self.calculate_actual_share(account_id, current_usage);

        // Fair share factor: how much of their fair share they've used
        // < 1.0 means underserved, > 1.0 means overserved
        let fair_share_factor = if target_share > 0.0 {
            actual_share / target_share
        } else {
            1.0 // No share assigned, neutral
        };

        // Convert to priority modifier
        // Using a sigmoid-like function to map factor to priority
        

        if fair_share_factor < 1.0 {
            // Underserved - boost priority
            let boost = ((1.0 - fair_share_factor) * 100.0) as i32;
            boost.min(self.config.max_priority_modifier)
        } else {
            // Overserved - lower priority
            let penalty = ((fair_share_factor - 1.0) * 100.0) as i32;
            (-penalty).max(self.config.min_priority_modifier)
        }
    }

    /// Get accounts sorted by fair share priority (most underserved first)
    pub fn prioritized_accounts(&self) -> Vec<(AccountId, f64)> {
        let mut accounts: Vec<_> = self
            .historical_usage
            .keys()
            .map(|id| {
                let target = self.get_target_share(id);
                let actual = self.get_historical_share(id);
                let ratio = if target > 0.0 { actual / target } else { 1.0 };
                (id.clone(), ratio)
            })
            .collect();

        // Sort by ratio (ascending) - lowest ratio = most underserved
        accounts.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        accounts
    }

    /// Record usage for fair share calculation
    pub fn record_usage(&mut self, account_id: &AccountId, usage: &HashMap<QuotaResource, f64>) {
        let snapshot = UsageSnapshot {
            timestamp: Utc::now(),
            usage: usage.iter().map(|(k, v)| (k.key(), *v)).collect(),
        };

        self.historical_usage
            .entry(account_id.clone())
            .or_default()
            .push(snapshot);

        // Trim old history
        self.trim_history(account_id);
    }

    /// Get the target share for an account
    pub fn get_target_share(&self, account_id: &AccountId) -> f64 {
        match &self.config.target_share_source {
            TargetShareSource::Equal => {
                let num_accounts = self.historical_usage.len().max(1);
                1.0 / num_accounts as f64
            }
            TargetShareSource::Proportional => {
                // This would need allocation information
                // For now, fall back to equal
                let num_accounts = self.historical_usage.len().max(1);
                1.0 / num_accounts as f64
            }
            TargetShareSource::Custom(shares) => shares.get(account_id).copied().unwrap_or(0.0),
        }
    }

    /// Set custom target shares
    pub fn set_custom_shares(&mut self, shares: HashMap<AccountId, f64>) {
        self.config.target_share_source = TargetShareSource::Custom(shares);
        self.invalidate_cache();
    }

    /// Update proportional shares based on allocations
    pub fn update_proportional_shares(&mut self, allocations: HashMap<AccountId, f64>) {
        let total: f64 = allocations.values().sum();
        if total > 0.0 {
            let normalized: HashMap<AccountId, f64> = allocations
                .into_iter()
                .map(|(k, v)| (k, v / total))
                .collect();
            self.config.target_share_source = TargetShareSource::Custom(normalized);
        }
        self.invalidate_cache();
    }

    /// Calculate the actual share of usage for an account
    fn calculate_actual_share(
        &self,
        account_id: &AccountId,
        current_usage: &HashMap<QuotaResource, f64>,
    ) -> f64 {
        // Combine historical and current usage with decay
        let historical = self.get_decayed_usage(account_id);
        let current: f64 = current_usage.values().sum();

        // Total usage across all accounts (historical)
        let total_historical: f64 = self
            .historical_usage
            .keys()
            .map(|id| self.get_decayed_usage(id))
            .sum();

        // Include current usage in the total as well to balance the calculation
        let total = (total_historical + current).max(1.0);
        (historical + current) / total
    }

    /// Get historical share for an account
    fn get_historical_share(&self, account_id: &AccountId) -> f64 {
        let account_usage = self.get_decayed_usage(account_id);
        let total_usage: f64 = self
            .historical_usage
            .keys()
            .map(|id| self.get_decayed_usage(id))
            .sum();

        if total_usage > 0.0 {
            account_usage / total_usage
        } else {
            0.0
        }
    }

    /// Get decayed usage for an account (older usage weighted less)
    fn get_decayed_usage(&self, account_id: &AccountId) -> f64 {
        let history = match self.historical_usage.get(account_id) {
            Some(h) => h,
            None => return 0.0,
        };

        let now = Utc::now();
        let halflife_secs = self.config.decay_halflife.num_seconds() as f64;

        history
            .iter()
            .filter(|s| now - s.timestamp < self.config.history_window)
            .map(|snapshot| {
                let age_secs = (now - snapshot.timestamp).num_seconds() as f64;
                let decay_factor = 0.5_f64.powf(age_secs / halflife_secs);
                let usage: f64 = snapshot.usage.values().sum();
                usage * decay_factor
            })
            .sum()
    }

    /// Trim old history outside the window
    fn trim_history(&mut self, account_id: &AccountId) {
        let now = Utc::now();
        let window = self.config.history_window;

        if let Some(history) = self.historical_usage.get_mut(account_id) {
            history.retain(|s| now - s.timestamp < window);
        }
    }

    /// Invalidate cached calculations
    fn invalidate_cache(&mut self) {
        self.cached_shares.clear();
        self.shares_calculated_at = None;
    }

    /// Get all historical usage snapshots for an account
    pub fn get_history(&self, account_id: &AccountId) -> Option<&Vec<UsageSnapshot>> {
        self.historical_usage.get(account_id)
    }

    /// Clear all historical data
    pub fn clear_history(&mut self) {
        self.historical_usage.clear();
        self.invalidate_cache();
    }

    /// Remove an account from tracking
    pub fn remove_account(&mut self, account_id: &AccountId) {
        self.historical_usage.remove(account_id);
        self.cached_shares.remove(account_id);
    }

    /// Get statistics about fair share distribution
    pub fn get_statistics(&self) -> FairShareStatistics {
        let accounts: Vec<_> = self.historical_usage.keys().collect();

        if accounts.is_empty() {
            return FairShareStatistics::default();
        }

        let shares: Vec<f64> = accounts
            .iter()
            .map(|id| self.get_historical_share(id))
            .collect();

        let mean = shares.iter().sum::<f64>() / shares.len() as f64;
        let variance = shares.iter().map(|s| (s - mean).powi(2)).sum::<f64>() / shares.len() as f64;
        let std_dev = variance.sqrt();

        // Gini coefficient for inequality measurement
        let gini = calculate_gini(&shares);

        FairShareStatistics {
            num_accounts: accounts.len(),
            mean_share: mean,
            std_deviation: std_dev,
            gini_coefficient: gini,
            most_underserved: self
                .prioritized_accounts()
                .first()
                .map(|(id, _)| id.clone()),
            most_overserved: self.prioritized_accounts().last().map(|(id, _)| id.clone()),
        }
    }
}

impl Default for FairShareCalculator {
    fn default() -> Self {
        Self::new(FairShareConfig::default())
    }
}

/// Statistics about fair share distribution
#[derive(Debug, Clone, Default)]
pub struct FairShareStatistics {
    /// Number of accounts being tracked
    pub num_accounts: usize,
    /// Mean share across accounts
    pub mean_share: f64,
    /// Standard deviation of shares
    pub std_deviation: f64,
    /// Gini coefficient (0 = perfect equality, 1 = maximum inequality)
    pub gini_coefficient: f64,
    /// Most underserved account
    pub most_underserved: Option<AccountId>,
    /// Most overserved account
    pub most_overserved: Option<AccountId>,
}

/// Calculate Gini coefficient for a set of values
fn calculate_gini(values: &[f64]) -> f64 {
    if values.is_empty() || values.len() == 1 {
        return 0.0;
    }

    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;

    if mean == 0.0 {
        return 0.0;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let sum: f64 = sorted
        .iter()
        .enumerate()
        .map(|(i, v)| (2.0 * (i + 1) as f64 - n - 1.0) * v)
        .sum();

    sum / (n * n * mean)
}

/// Integration helper to apply fair share to job scheduling
pub struct FairShareSchedulerIntegration {
    calculator: FairShareCalculator,
    /// Weight for fair share in final priority calculation (0.0-1.0)
    fair_share_weight: f64,
}

impl FairShareSchedulerIntegration {
    /// Create a new integration with the given calculator
    pub fn new(calculator: FairShareCalculator) -> Self {
        Self {
            calculator,
            fair_share_weight: 0.5,
        }
    }

    /// Set the weight for fair share in priority calculation
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.fair_share_weight = weight.clamp(0.0, 1.0);
        self
    }

    /// Calculate adjusted priority for a job
    ///
    /// Returns the base priority modified by fair share considerations
    pub fn adjust_priority(
        &self,
        base_priority: i32,
        account_id: &AccountId,
        current_usage: &HashMap<QuotaResource, f64>,
    ) -> i32 {
        let fair_share_modifier = self
            .calculator
            .calculate_priority_modifier(account_id, current_usage);

        let weighted_modifier = (fair_share_modifier as f64 * self.fair_share_weight) as i32;
        base_priority + weighted_modifier
    }

    /// Record usage for an account
    pub fn record_usage(&mut self, account_id: &AccountId, usage: &HashMap<QuotaResource, f64>) {
        self.calculator.record_usage(account_id, usage);
    }

    /// Get the underlying calculator
    pub fn calculator(&self) -> &FairShareCalculator {
        &self.calculator
    }

    /// Get mutable access to the calculator
    pub fn calculator_mut(&mut self) -> &mut FairShareCalculator {
        &mut self.calculator
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fair_share_calculator_creation() {
        let config = FairShareConfig::default();
        let calc = FairShareCalculator::new(config);
        assert!(calc.historical_usage.is_empty());
    }

    #[test]
    fn test_record_usage() {
        let mut calc = FairShareCalculator::new(FairShareConfig::default());

        let mut usage = HashMap::new();
        usage.insert(QuotaResource::CpuHours, 10.0);

        calc.record_usage(&"acc-1".to_string(), &usage);

        assert!(calc.historical_usage.contains_key("acc-1"));
        assert_eq!(calc.historical_usage.get("acc-1").unwrap().len(), 1);
    }

    #[test]
    fn test_equal_shares() {
        let mut calc = FairShareCalculator::new(FairShareConfig::default());

        // Record usage for two accounts
        let mut usage = HashMap::new();
        usage.insert(QuotaResource::CpuHours, 10.0);
        calc.record_usage(&"acc-1".to_string(), &usage);
        calc.record_usage(&"acc-2".to_string(), &usage);

        // Both should have equal target shares
        let share1 = calc.get_target_share(&"acc-1".to_string());
        let share2 = calc.get_target_share(&"acc-2".to_string());

        assert!((share1 - 0.5).abs() < 0.001);
        assert!((share2 - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_custom_shares() {
        let mut calc = FairShareCalculator::new(FairShareConfig::default());

        let mut shares = HashMap::new();
        shares.insert("acc-1".to_string(), 0.7);
        shares.insert("acc-2".to_string(), 0.3);
        calc.set_custom_shares(shares);

        assert!((calc.get_target_share(&"acc-1".to_string()) - 0.7).abs() < 0.001);
        assert!((calc.get_target_share(&"acc-2".to_string()) - 0.3).abs() < 0.001);
    }

    #[test]
    fn test_priority_modifier_underserved() {
        let mut calc = FairShareCalculator::new(FairShareConfig::default());

        // acc-1 uses a lot, acc-2 uses little
        let mut heavy_usage = HashMap::new();
        heavy_usage.insert(QuotaResource::CpuHours, 100.0);

        let mut light_usage = HashMap::new();
        light_usage.insert(QuotaResource::CpuHours, 10.0);

        calc.record_usage(&"acc-1".to_string(), &heavy_usage);
        calc.record_usage(&"acc-2".to_string(), &light_usage);

        // acc-2 should get positive modifier (underserved)
        let modifier = calc.calculate_priority_modifier(&"acc-2".to_string(), &light_usage);
        assert!(
            modifier > 0,
            "Underserved account should get priority boost"
        );

        // acc-1 should get negative modifier (overserved)
        let modifier = calc.calculate_priority_modifier(&"acc-1".to_string(), &heavy_usage);
        assert!(
            modifier < 0,
            "Overserved account should get priority penalty"
        );
    }

    #[test]
    fn test_prioritized_accounts() {
        let mut calc = FairShareCalculator::new(FairShareConfig::default());

        // Set up different usage levels
        for i in 1..=5 {
            let mut usage = HashMap::new();
            usage.insert(QuotaResource::CpuHours, (i * 10) as f64);
            calc.record_usage(&format!("acc-{}", i), &usage);
        }

        let prioritized = calc.prioritized_accounts();

        // First should be lowest usage (most underserved)
        assert_eq!(prioritized.first().unwrap().0, "acc-1");
        // Last should be highest usage (most overserved)
        assert_eq!(prioritized.last().unwrap().0, "acc-5");
    }

    #[test]
    fn test_decay() {
        let config = FairShareConfig::default().with_decay_halflife(Duration::hours(1));
        let mut calc = FairShareCalculator::new(config);

        // Record usage
        let mut usage = HashMap::new();
        usage.insert(QuotaResource::CpuHours, 100.0);
        calc.record_usage(&"acc-1".to_string(), &usage);

        // Get decayed usage immediately (should be close to 100)
        let decayed = calc.get_decayed_usage(&"acc-1".to_string());
        assert!(decayed > 90.0 && decayed <= 100.0);
    }

    #[test]
    fn test_statistics() {
        let mut calc = FairShareCalculator::new(FairShareConfig::default());

        // Create unequal usage
        for i in 1..=10 {
            let mut usage = HashMap::new();
            usage.insert(QuotaResource::CpuHours, (i * i) as f64);
            calc.record_usage(&format!("acc-{}", i), &usage);
        }

        let stats = calc.get_statistics();
        assert_eq!(stats.num_accounts, 10);
        assert!(stats.gini_coefficient > 0.0); // Should show inequality
    }

    #[test]
    fn test_gini_coefficient() {
        // Perfect equality
        let equal = vec![1.0, 1.0, 1.0, 1.0];
        assert!(calculate_gini(&equal).abs() < 0.001);

        // High inequality
        let unequal = vec![0.0, 0.0, 0.0, 100.0];
        assert!(calculate_gini(&unequal) > 0.5);
    }

    #[test]
    fn test_scheduler_integration() {
        let calc = FairShareCalculator::new(FairShareConfig::default());
        let mut integration = FairShareSchedulerIntegration::new(calc).with_weight(0.5);

        let mut usage = HashMap::new();
        usage.insert(QuotaResource::CpuHours, 50.0);
        integration.record_usage(&"acc-1".to_string(), &usage);

        // Base priority 100
        let adjusted = integration.adjust_priority(100, &"acc-1".to_string(), &usage);

        // Should be modified based on fair share
        // With only one account and equal target, modifier should be close to 0
        assert!((adjusted - 100).abs() <= 10);
    }

    #[test]
    fn test_remove_account() {
        let mut calc = FairShareCalculator::new(FairShareConfig::default());

        let mut usage = HashMap::new();
        usage.insert(QuotaResource::CpuHours, 10.0);
        calc.record_usage(&"acc-1".to_string(), &usage);
        calc.record_usage(&"acc-2".to_string(), &usage);

        assert_eq!(calc.historical_usage.len(), 2);

        calc.remove_account(&"acc-1".to_string());

        assert_eq!(calc.historical_usage.len(), 1);
        assert!(!calc.historical_usage.contains_key("acc-1"));
    }

    #[test]
    fn test_clear_history() {
        let mut calc = FairShareCalculator::new(FairShareConfig::default());

        let mut usage = HashMap::new();
        usage.insert(QuotaResource::CpuHours, 10.0);
        calc.record_usage(&"acc-1".to_string(), &usage);

        assert!(!calc.historical_usage.is_empty());

        calc.clear_history();

        assert!(calc.historical_usage.is_empty());
    }

    #[test]
    fn test_proportional_shares() {
        let mut calc = FairShareCalculator::new(FairShareConfig::default());

        // Set up allocations
        let mut allocations = HashMap::new();
        allocations.insert("acc-1".to_string(), 100.0);
        allocations.insert("acc-2".to_string(), 300.0);
        calc.update_proportional_shares(allocations);

        // acc-1 should have 25% share
        assert!((calc.get_target_share(&"acc-1".to_string()) - 0.25).abs() < 0.001);
        // acc-2 should have 75% share
        assert!((calc.get_target_share(&"acc-2".to_string()) - 0.75).abs() < 0.001);
    }
}
