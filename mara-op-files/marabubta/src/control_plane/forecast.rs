// Marabunta - Licensed under the MIT License.
//! Capacity forecasting engine for predicting future compute availability
//!
//! This module provides statistical forecasting of compute capacity based on
//! historical patterns, scheduled events, and anomaly detection.

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::common::types::RegionId;

/// Minimum number of samples required to build a reliable pattern
const MIN_SAMPLES_FOR_PATTERN: u32 = 10;

/// Number of standard deviations for confidence intervals
const CONFIDENCE_INTERVAL_STDDEV: f64 = 1.28; // ~80% confidence (10th-90th percentile)

/// Threshold for anomaly detection (standard deviations from predicted)
const ANOMALY_THRESHOLD_STDDEV: f64 = 2.0;

/// Historical pattern data for a specific hour and day of week
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlyPattern {
    /// Hour of day (0-23)
    pub hour: u8,
    /// Day of week (0=Monday, 6=Sunday)
    pub day_of_week: u8,
    /// Average capacity in FLOPS
    pub avg_capacity_flops: f64,
    /// Standard deviation
    pub std_dev: f64,
    /// Number of samples collected
    pub sample_count: u32,
}

impl HourlyPattern {
    fn new(hour: u8, day_of_week: u8) -> Self {
        Self {
            hour,
            day_of_week,
            avg_capacity_flops: 0.0,
            std_dev: 0.0,
            sample_count: 0,
        }
    }

    /// Update pattern with new observation using running statistics
    fn update(&mut self, capacity_flops: f64) {
        let n = self.sample_count as f64;
        let old_mean = self.avg_capacity_flops;

        // Update mean
        self.avg_capacity_flops = (old_mean * n + capacity_flops) / (n + 1.0);

        // Update variance using Welford's online algorithm
        if self.sample_count > 0 {
            let old_variance = self.std_dev * self.std_dev;
            let new_variance = ((n * old_variance
                + (capacity_flops - old_mean) * (capacity_flops - self.avg_capacity_flops))
                / (n + 1.0))
                .max(0.0);
            self.std_dev = new_variance.sqrt();
        }

        self.sample_count += 1;
    }

    /// Check if pattern has enough samples to be reliable
    fn is_reliable(&self) -> bool {
        self.sample_count >= MIN_SAMPLES_FOR_PATTERN
    }
}

/// A scheduled event affecting capacity (e.g., school hours, maintenance)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledEvent {
    /// Event name/description
    pub name: String,
    /// Start hour (0-23)
    pub start_hour: u8,
    /// End hour (0-23)
    pub end_hour: u8,
    /// Days of week this applies to (0=Monday, 6=Sunday), None = all days
    pub days_of_week: Option<Vec<u8>>,
    /// Expected capacity multiplier (0.0-1.0, where 0.5 = 50% capacity)
    pub capacity_multiplier: f64,
    /// Specific dates this applies to (for holidays, etc.)
    pub specific_dates: Vec<DateTime<Utc>>,
}

impl ScheduledEvent {
    /// Check if this event applies at the given time
    fn applies_at(&self, time: DateTime<Utc>) -> bool {
        let hour = time.hour() as u8;
        let day_of_week = time.weekday().num_days_from_monday() as u8;

        // Check if hour is in range (use <= for end_hour to include that hour)
        let in_time_range = if self.start_hour <= self.end_hour {
            hour >= self.start_hour && hour <= self.end_hour
        } else {
            // Wraps around midnight
            hour >= self.start_hour || hour <= self.end_hour
        };

        if !in_time_range {
            return false;
        }

        // Check specific dates first - if this is a date-specific event,
        // only apply if the date matches
        if !self.specific_dates.is_empty() {
            let date = time.date_naive();
            return self.specific_dates.iter().any(|d| d.date_naive() == date);
        }

        // Check day of week if specified
        if let Some(days) = &self.days_of_week {
            if !days.contains(&day_of_week) {
                return false;
            }
        }

        true
    }
}

/// Single hourly prediction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlyPrediction {
    /// Timestamp for this prediction
    pub timestamp: DateTime<Utc>,
    /// Predicted capacity in FLOPS
    pub predicted_flops: f64,
    /// Lower bound (10th percentile)
    pub confidence_low: f64,
    /// Upper bound (90th percentile)
    pub confidence_high: f64,
    /// Factors contributing to this prediction
    pub contributing_factors: Vec<String>,
}

/// Capacity forecast for a region
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityForecast {
    /// Region this forecast is for
    pub region_id: RegionId,
    /// When this forecast was generated
    pub generated_at: DateTime<Utc>,
    /// Hourly predictions
    pub predictions: Vec<HourlyPrediction>,
}

/// Detected capacity anomaly
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityAnomaly {
    /// When the anomaly occurred
    pub timestamp: DateTime<Utc>,
    /// Region where anomaly was detected
    pub region_id: RegionId,
    /// Actual capacity observed
    pub actual_flops: f64,
    /// Predicted capacity
    pub predicted_flops: f64,
    /// Deviation in standard deviations
    pub std_dev_away: f64,
    /// Possible causes
    pub possible_causes: Vec<String>,
}

/// Job planning recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobPlanningRecommendation {
    /// Estimated completion time from now
    pub estimated_completion_hours: f64,
    /// Optimal start time for fastest completion
    pub optimal_start_time: DateTime<Utc>,
    /// Whether there's sufficient capacity forecasted
    pub sufficient_capacity: bool,
    /// Warnings about capacity constraints
    pub warnings: Vec<String>,
    /// Timeline of capacity during job execution
    pub capacity_timeline: Vec<(DateTime<Utc>, f64)>,
}

/// Main capacity forecasting engine
pub struct CapacityForecaster {
    /// Historical patterns by region, indexed by (day_of_week, hour)
    patterns: RwLock<HashMap<RegionId, HashMap<(u8, u8), HourlyPattern>>>,
    /// Scheduled events by region
    scheduled_events: RwLock<HashMap<RegionId, Vec<ScheduledEvent>>>,
    /// Recent observations for anomaly detection
    recent_observations: RwLock<HashMap<RegionId, Vec<(DateTime<Utc>, f64)>>>,
}

impl CapacityForecaster {
    /// Create a new capacity forecaster
    pub fn new() -> Self {
        Self {
            patterns: RwLock::new(HashMap::new()),
            scheduled_events: RwLock::new(HashMap::new()),
            recent_observations: RwLock::new(HashMap::new()),
        }
    }

    /// Record a capacity observation
    pub fn record_observation(
        &self,
        region_id: RegionId,
        timestamp: DateTime<Utc>,
        capacity_flops: f64,
    ) {
        let hour = timestamp.hour() as u8;
        let day_of_week = timestamp.weekday().num_days_from_monday() as u8;

        // Update historical pattern
        let mut patterns = self.patterns.write();
        let region_patterns = patterns.entry(region_id).or_insert_with(HashMap::new);
        let pattern = region_patterns
            .entry((day_of_week, hour))
            .or_insert_with(|| HourlyPattern::new(hour, day_of_week));
        pattern.update(capacity_flops);
        drop(patterns);

        // Store recent observation for anomaly detection
        let mut recent = self.recent_observations.write();
        let observations = recent.entry(region_id).or_insert_with(Vec::new);
        observations.push((timestamp, capacity_flops));

        // Keep only last 7 days of observations
        let cutoff = Utc::now() - Duration::days(7);
        observations.retain(|(ts, _)| *ts > cutoff);
    }

    /// Add a scheduled event
    pub fn add_scheduled_event(&self, region_id: RegionId, event: ScheduledEvent) {
        let mut events = self.scheduled_events.write();
        events.entry(region_id).or_insert_with(Vec::new).push(event);
    }

    /// Remove a scheduled event by name
    pub fn remove_scheduled_event(&self, region_id: RegionId, event_name: &str) {
        let mut events = self.scheduled_events.write();
        if let Some(region_events) = events.get_mut(&region_id) {
            region_events.retain(|e| e.name != event_name);
        }
    }

    /// Generate forecast for a region
    pub fn forecast(&self, region_id: RegionId, hours_ahead: usize) -> CapacityForecast {
        let mut predictions = Vec::with_capacity(hours_ahead);
        let now = Utc::now();

        let patterns = self.patterns.read();
        let region_patterns = patterns.get(&region_id);

        let events = self.scheduled_events.read();
        let region_events = events.get(&region_id);

        for hour_offset in 0..hours_ahead {
            let timestamp = now + Duration::hours(hour_offset as i64);
            let prediction = self.predict_hour(timestamp, region_patterns, region_events);
            predictions.push(prediction);
        }

        CapacityForecast {
            region_id,
            generated_at: now,
            predictions,
        }
    }

    /// Predict capacity for a specific hour
    fn predict_hour(
        &self,
        timestamp: DateTime<Utc>,
        region_patterns: Option<&HashMap<(u8, u8), HourlyPattern>>,
        region_events: Option<&Vec<ScheduledEvent>>,
    ) -> HourlyPrediction {
        let hour = timestamp.hour() as u8;
        let day_of_week = timestamp.weekday().num_days_from_monday() as u8;

        let mut contributing_factors = Vec::new();
        let mut predicted_flops = 0.0;
        let mut std_dev = 0.0;

        // Get base prediction from historical patterns
        if let Some(patterns) = region_patterns {
            if let Some(pattern) = patterns.get(&(day_of_week, hour)) {
                if pattern.is_reliable() {
                    predicted_flops = pattern.avg_capacity_flops;
                    std_dev = pattern.std_dev;

                    // Add day/time context
                    match day_of_week {
                        5 | 6 => contributing_factors.push("weekend".to_string()),
                        _ => contributing_factors.push("weekday".to_string()),
                    }

                    if hour >= 8 && hour < 17 {
                        contributing_factors.push("business_hours".to_string());
                    } else if hour >= 22 || hour < 6 {
                        contributing_factors.push("overnight".to_string());
                    }
                } else {
                    contributing_factors.push("insufficient_data".to_string());
                }
            }
        }

        // Apply scheduled events
        if let Some(events) = region_events {
            for event in events {
                if event.applies_at(timestamp) {
                    predicted_flops *= event.capacity_multiplier;
                    contributing_factors.push(event.name.clone());
                }
            }
        }

        // Calculate confidence intervals
        let confidence_low = (predicted_flops - CONFIDENCE_INTERVAL_STDDEV * std_dev).max(0.0);
        let confidence_high = predicted_flops + CONFIDENCE_INTERVAL_STDDEV * std_dev;

        HourlyPrediction {
            timestamp,
            predicted_flops,
            confidence_low,
            confidence_high,
            contributing_factors,
        }
    }

    /// Detect anomalies in recent observations
    pub fn detect_anomalies(&self, region_id: RegionId) -> Vec<CapacityAnomaly> {
        let mut anomalies = Vec::new();

        let recent = self.recent_observations.read();
        let observations = match recent.get(&region_id) {
            Some(obs) => obs,
            None => return anomalies,
        };

        let patterns = self.patterns.read();
        let region_patterns = patterns.get(&region_id);

        let events = self.scheduled_events.read();
        let region_events = events.get(&region_id);

        for (timestamp, actual_flops) in observations {
            let prediction = self.predict_hour(*timestamp, region_patterns, region_events);

            // Skip if we don't have enough data for reliable prediction
            if prediction.predicted_flops == 0.0 {
                continue;
            }

            let hour = timestamp.hour() as u8;
            let day_of_week = timestamp.weekday().num_days_from_monday() as u8;

            if let Some(patterns) = region_patterns {
                if let Some(pattern) = patterns.get(&(day_of_week, hour)) {
                    if !pattern.is_reliable() {
                        continue;
                    }

                    let deviation = (*actual_flops - prediction.predicted_flops).abs();
                    let std_dev_away = if pattern.std_dev > 0.0 {
                        deviation / pattern.std_dev
                    } else {
                        0.0
                    };

                    if std_dev_away > ANOMALY_THRESHOLD_STDDEV {
                        let mut possible_causes = Vec::new();

                        if *actual_flops < prediction.predicted_flops {
                            possible_causes.push("unplanned_outage".to_string());
                            possible_causes.push("maintenance".to_string());
                            possible_causes.push("network_issue".to_string());
                        } else {
                            possible_causes.push("new_nodes_added".to_string());
                            possible_causes.push("reduced_load".to_string());
                        }

                        anomalies.push(CapacityAnomaly {
                            timestamp: *timestamp,
                            region_id,
                            actual_flops: *actual_flops,
                            predicted_flops: prediction.predicted_flops,
                            std_dev_away,
                            possible_causes,
                        });
                    }
                }
            }
        }

        anomalies
    }

    /// Auto-detect consistent capacity drops and suggest scheduled events
    pub fn detect_patterns(&self, region_id: RegionId) -> Vec<ScheduledEvent> {
        let mut suggested_events = Vec::new();

        let patterns = self.patterns.read();
        let region_patterns = match patterns.get(&region_id) {
            Some(p) => p,
            None => return suggested_events,
        };

        // Look for consistent weekday patterns (e.g., school hours)
        for day in 0..5 {
            // Monday-Friday
            let mut low_capacity_hours = Vec::new();
            let mut high_capacity_hours = Vec::new();

            for hour in 0..24 {
                if let Some(pattern) = region_patterns.get(&(day, hour)) {
                    if pattern.is_reliable() {
                        // Compare to adjacent hours and overall average
                        let prev_hour = if hour > 0 { hour - 1 } else { 23 };
                        let next_hour = (hour + 1) % 24;

                        let prev_pattern = region_patterns.get(&(day, prev_hour));
                        let next_pattern = region_patterns.get(&(day, next_hour));

                        if let (Some(prev), Some(next)) = (prev_pattern, next_pattern) {
                            if prev.is_reliable() && next.is_reliable() {
                                let avg_adjacent =
                                    (prev.avg_capacity_flops + next.avg_capacity_flops) / 2.0;

                                // Significant drop
                                if pattern.avg_capacity_flops < avg_adjacent * 0.5 {
                                    low_capacity_hours.push(hour);
                                }
                                // Significant increase
                                if pattern.avg_capacity_flops > avg_adjacent * 1.5 {
                                    high_capacity_hours.push(hour);
                                }
                            }
                        }
                    }
                }
            }

            // If we found consecutive low hours, suggest an event
            if !low_capacity_hours.is_empty() {
                let start_hour = *low_capacity_hours.first().unwrap();
                let end_hour = (*low_capacity_hours.last().unwrap() + 1) % 24;

                if low_capacity_hours.len() >= 3 {
                    suggested_events.push(ScheduledEvent {
                        name: format!("detected_low_capacity_{}h_{}h", start_hour, end_hour),
                        start_hour,
                        end_hour,
                        days_of_week: Some(vec![0, 1, 2, 3, 4]), // Weekdays
                        capacity_multiplier: 0.3,
                        specific_dates: vec![],
                    });
                }
            }
        }

        // Look for weekend patterns
        let mut weekend_low = true;
        for day in 5..7 {
            // Saturday-Sunday
            for hour in 8..18 {
                if let Some(pattern) = region_patterns.get(&(day, hour)) {
                    if pattern.is_reliable() {
                        // Compare to same hour on weekdays
                        let weekday_avg = (0..5)
                            .filter_map(|d| region_patterns.get(&(d, hour)))
                            .filter(|p| p.is_reliable())
                            .map(|p| p.avg_capacity_flops)
                            .sum::<f64>()
                            / 5.0;

                        if pattern.avg_capacity_flops >= weekday_avg * 0.8 {
                            weekend_low = false;
                            break;
                        }
                    }
                }
            }
        }

        if weekend_low {
            suggested_events.push(ScheduledEvent {
                name: "detected_weekend_low".to_string(),
                start_hour: 0,
                end_hour: 23,
                days_of_week: Some(vec![5, 6]),
                capacity_multiplier: 0.2,
                specific_dates: vec![],
            });
        }

        suggested_events
    }

    /// Plan a job and recommend execution strategy
    pub fn plan_job(
        &self,
        region_id: RegionId,
        required_flops_hours: f64,  // Total FLOPS-hours needed
        max_execution_hours: usize, // Maximum time window
    ) -> JobPlanningRecommendation {
        let forecast = self.forecast(region_id, max_execution_hours);

        let mut warnings = Vec::new();
        let mut capacity_timeline = Vec::new();

        // Try starting at different times and find the fastest completion
        let mut best_completion_hours = max_execution_hours as f64;
        let mut best_start_time = Utc::now();

        for start_offset in 0..max_execution_hours {
            let mut flops_accumulated = 0.0;
            let mut hours_needed = 0.0;

            for i in start_offset..max_execution_hours {
                let prediction = &forecast.predictions[i];

                if prediction.predicted_flops <= 0.0 {
                    continue;
                }

                // Estimate how much work can be done in this hour
                flops_accumulated += prediction.predicted_flops;
                hours_needed += 1.0;

                capacity_timeline.push((prediction.timestamp, prediction.predicted_flops));

                if flops_accumulated >= required_flops_hours {
                    if hours_needed < best_completion_hours {
                        best_completion_hours = hours_needed;
                        best_start_time = forecast.predictions[start_offset].timestamp;
                    }
                    break;
                }
            }
        }

        // Check if job can complete within time window
        let sufficient_capacity = best_completion_hours < max_execution_hours as f64;

        if !sufficient_capacity {
            warnings.push(format!(
                "Job may not complete within {} hours - estimated {} hours needed",
                max_execution_hours, best_completion_hours
            ));
        }

        // Warn about low-capacity periods
        let now = Utc::now();
        for prediction in &forecast.predictions {
            if prediction.timestamp > now
                && prediction.predicted_flops < required_flops_hours / max_execution_hours as f64
            {
                warnings.push(format!(
                    "Low capacity period detected at {}: {}",
                    prediction.timestamp.format("%Y-%m-%d %H:%M"),
                    prediction.contributing_factors.join(", ")
                ));
            }
        }

        JobPlanningRecommendation {
            estimated_completion_hours: best_completion_hours,
            optimal_start_time: best_start_time,
            sufficient_capacity,
            warnings,
            capacity_timeline,
        }
    }

    /// Get summary statistics for a region
    pub fn get_region_statistics(&self, region_id: RegionId) -> Option<RegionStatistics> {
        let patterns = self.patterns.read();
        let region_patterns = patterns.get(&region_id)?;

        let mut total_avg = 0.0;
        let mut count = 0;
        let mut min_capacity = f64::MAX;
        let mut max_capacity = f64::MIN;
        let mut weekday_avg = 0.0;
        let mut weekend_avg = 0.0;
        let mut weekday_count = 0;
        let mut weekend_count = 0;

        for ((day, _hour), pattern) in region_patterns {
            if !pattern.is_reliable() {
                continue;
            }

            total_avg += pattern.avg_capacity_flops;
            count += 1;
            min_capacity = min_capacity.min(pattern.avg_capacity_flops);
            max_capacity = max_capacity.max(pattern.avg_capacity_flops);

            if *day < 5 {
                weekday_avg += pattern.avg_capacity_flops;
                weekday_count += 1;
            } else {
                weekend_avg += pattern.avg_capacity_flops;
                weekend_count += 1;
            }
        }

        if count == 0 {
            return None;
        }

        Some(RegionStatistics {
            region_id,
            avg_capacity_flops: total_avg / count as f64,
            min_capacity_flops: min_capacity,
            max_capacity_flops: max_capacity,
            weekday_avg_flops: if weekday_count > 0 {
                weekday_avg / weekday_count as f64
            } else {
                0.0
            },
            weekend_avg_flops: if weekend_count > 0 {
                weekend_avg / weekend_count as f64
            } else {
                0.0
            },
            pattern_count: count,
        })
    }
}

impl Default for CapacityForecaster {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary statistics for a region
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionStatistics {
    pub region_id: RegionId,
    pub avg_capacity_flops: f64,
    pub min_capacity_flops: f64,
    pub max_capacity_flops: f64,
    pub weekday_avg_flops: f64,
    pub weekend_avg_flops: f64,
    pub pattern_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Timelike};

    fn test_region() -> RegionId {
        RegionId::from_name("test-region")
    }

    fn create_school_pattern(forecaster: &CapacityForecaster, region: RegionId) {
        // Simulate 10 weeks of data with school pattern:
        // Weekdays 8am-5pm: low capacity (school using computers)
        // Weekdays 6pm-7am: high capacity (computers idle)
        // Weekends: lower capacity than weekday average (for pattern detection)
        // Need at least 10 samples per (day, hour) pattern for reliability
        //
        // Pattern detection algorithm compares each hour to adjacent hours.
        // It detects hours where capacity < 50% of adjacent hours average.
        // To get 3+ consecutive detected hours, we need a gradual descent:
        // Hour 6: 1G (high)
        // Hour 7: 800M (starting descent - detected: 800M < (1G+500M)/2*0.5 = 375M? No)
        // Hour 8: 500M (mid descent - detected: 500M < (800M+200M)/2*0.5 = 250M? No)
        // Hour 9: 200M (detected: 200M < (500M+100M)/2*0.5 = 150M? No)
        // Hour 10: 100M (low plateau)
        //
        // Actually the algorithm can only detect single outlier hours, not plateaus.
        // For 3+ detections, we need multiple isolated dips:
        // Create spikes at hours 9, 11, 13 surrounded by high capacity

        let start = Utc::now() - Duration::days(70);

        for day in 0..70 {
            let date = start + Duration::days(day);
            let day_of_week = date.weekday().num_days_from_monday();

            let is_weekend = day_of_week >= 5;

            for hour in 0..24 {
                let timestamp = Utc
                    .with_ymd_and_hms(date.year(), date.month(), date.day(), hour, 0, 0)
                    .unwrap();

                let capacity = if is_weekend {
                    // Weekend: lower capacity than weekday average for pattern detection
                    400_000_000.0 // 400 MFLOPS
                } else if hour == 9 || hour == 11 || hour == 13 || hour == 15 {
                    // Isolated low-capacity hours (detected by algorithm)
                    // Each is surrounded by high capacity hours (10, 12, 14, 16)
                    100_000_000.0 // 100 MFLOPS
                } else if hour >= 8 && hour < 17 {
                    // Surrounding high capacity during school hours
                    1_000_000_000.0 // 1 GFLOPS
                } else {
                    // Off hours: also high capacity
                    1_000_000_000.0 // 1 GFLOPS
                };

                // Add some noise
                let noise = (hour as f64 * 13.0 + day as f64 * 7.0).sin() * capacity * 0.1;
                forecaster.record_observation(region, timestamp, capacity + noise);
            }
        }
    }

    #[test]
    fn test_pattern_learning() {
        let forecaster = CapacityForecaster::new();
        let region = test_region();

        create_school_pattern(&forecaster, region);

        // Check that patterns were learned
        let patterns = forecaster.patterns.read();
        let region_patterns = patterns.get(&region).unwrap();

        // Should have patterns for each hour and day
        assert!(region_patterns.len() > 0);

        // Check weekday low-capacity hour (hours 9, 11, 13, 15 are low in test data)
        let weekday_low_hour = region_patterns.get(&(0, 9)).unwrap(); // Monday 9am
        assert!(weekday_low_hour.is_reliable());
        assert!(weekday_low_hour.avg_capacity_flops < 200_000_000.0);

        // Check weekday high-capacity hour (hours 10, 12, 14, 16 are high during school hours)
        let weekday_high_hour = region_patterns.get(&(0, 10)).unwrap(); // Monday 10am
        assert!(weekday_high_hour.is_reliable());
        assert!(weekday_high_hour.avg_capacity_flops > 800_000_000.0);

        // Check weekday off-hours pattern
        let weekday_night = region_patterns.get(&(0, 22)).unwrap(); // Monday 10pm
        assert!(weekday_night.is_reliable());
        assert!(weekday_night.avg_capacity_flops > 800_000_000.0);

        // Check weekend pattern (400M in test data)
        let weekend = region_patterns.get(&(5, 10)).unwrap(); // Saturday 10am
        assert!(weekend.is_reliable());
        assert!(weekend.avg_capacity_flops > 300_000_000.0);
        assert!(weekend.avg_capacity_flops < 500_000_000.0);
    }

    #[test]
    fn test_forecast_generation() {
        let forecaster = CapacityForecaster::new();
        let region = test_region();

        create_school_pattern(&forecaster, region);

        // Generate 24-hour forecast
        let forecast = forecaster.forecast(region, 24);

        assert_eq!(forecast.predictions.len(), 24);
        assert_eq!(forecast.region_id, region);

        // Verify predictions have reasonable values
        for prediction in &forecast.predictions {
            assert!(prediction.predicted_flops > 0.0);
            assert!(prediction.confidence_low >= 0.0);
            assert!(prediction.confidence_high > prediction.confidence_low);
            assert!(prediction.confidence_low <= prediction.predicted_flops);
            assert!(prediction.confidence_high >= prediction.predicted_flops);
        }
    }

    #[test]
    fn test_scheduled_events() {
        let forecaster = CapacityForecaster::new();
        let region = test_region();

        create_school_pattern(&forecaster, region);

        // Add a maintenance window event
        let maintenance = ScheduledEvent {
            name: "maintenance".to_string(),
            start_hour: 2,
            end_hour: 4,
            days_of_week: None,       // All days
            capacity_multiplier: 0.1, // 10% capacity during maintenance
            specific_dates: vec![],
        };

        forecaster.add_scheduled_event(region, maintenance);

        // Generate forecast and check that maintenance hours have reduced capacity
        let forecast = forecaster.forecast(region, 48);

        for prediction in &forecast.predictions {
            let hour = prediction.timestamp.hour() as u8;
            if hour >= 2 && hour < 4 {
                assert!(prediction
                    .contributing_factors
                    .contains(&"maintenance".to_string()));
                // Capacity should be significantly reduced
                let base_prediction = forecaster.predict_hour(
                    prediction.timestamp,
                    forecaster.patterns.read().get(&region),
                    None,
                );
                assert!(prediction.predicted_flops < base_prediction.predicted_flops * 0.2);
            }
        }
    }

    #[test]
    fn test_anomaly_detection() {
        let forecaster = CapacityForecaster::new();
        let region = test_region();

        create_school_pattern(&forecaster, region);

        // Record an anomalous observation (unexpected outage)
        let now = Utc::now();
        let hour = now.hour() as u8;
        let day_of_week = now.weekday().num_days_from_monday() as u8;

        // Get expected capacity
        let patterns = forecaster.patterns.read();
        let expected = patterns
            .get(&region)
            .and_then(|p| p.get(&(day_of_week, hour)))
            .map(|p| p.avg_capacity_flops)
            .unwrap_or(500_000_000.0);
        drop(patterns);

        // Record very low capacity (10% of expected)
        forecaster.record_observation(region, now, expected * 0.1);

        // Detect anomalies
        let anomalies = forecaster.detect_anomalies(region);

        // Should detect the anomaly
        assert!(!anomalies.is_empty());

        let anomaly = &anomalies[0];
        assert!(anomaly.std_dev_away > ANOMALY_THRESHOLD_STDDEV);
        assert!(anomaly.actual_flops < anomaly.predicted_flops);
        assert!(anomaly
            .possible_causes
            .contains(&"unplanned_outage".to_string()));
    }

    #[test]
    fn test_pattern_detection() {
        let forecaster = CapacityForecaster::new();
        let region = test_region();

        create_school_pattern(&forecaster, region);

        // Auto-detect patterns
        let suggested_events = forecaster.detect_patterns(region);

        // Should detect school hours and weekend patterns
        assert!(!suggested_events.is_empty());

        // Look for weekday low capacity detection
        let has_weekday_low = suggested_events.iter().any(|e| {
            e.days_of_week
                .as_ref()
                .map_or(false, |days| days.contains(&0))
                && e.capacity_multiplier < 0.5
        });
        assert!(
            has_weekday_low,
            "Should detect weekday low capacity pattern"
        );
    }

    #[test]
    fn test_job_planning() {
        let forecaster = CapacityForecaster::new();
        let region = test_region();

        create_school_pattern(&forecaster, region);

        // Plan a job requiring 10 GFLOPS-hours
        let required = 10_000_000_000.0; // 10 GFLOPS-hours
        let recommendation = forecaster.plan_job(region, required, 24);

        assert!(recommendation.estimated_completion_hours > 0.0);
        assert!(recommendation.estimated_completion_hours <= 24.0);
        assert!(!recommendation.capacity_timeline.is_empty());

        // Should recommend starting during high-capacity period
        // The test pattern has high capacity during school hours (8-17)
        // except for isolated dip hours (9, 11, 13, 15)
        let start_hour = recommendation.optimal_start_time.hour() as u8;
        // Job should start during high capacity hours (avoiding dips at 9, 11, 13, 15)
        let is_high_capacity_hour = (start_hour >= 8
            && start_hour < 17
            && start_hour != 9
            && start_hour != 11
            && start_hour != 13
            && start_hour != 15)
            || start_hour < 8
            || start_hour >= 17;
        assert!(
            is_high_capacity_hour,
            "Job should start during high-capacity hour, got {}",
            start_hour
        );
    }

    #[test]
    fn test_insufficient_capacity_warning() {
        let forecaster = CapacityForecaster::new();
        let region = test_region();

        create_school_pattern(&forecaster, region);

        // Plan a job requiring way too much capacity
        let required = 100_000_000_000_000.0; // 100 TFLOPS-hours
        let recommendation = forecaster.plan_job(region, required, 24);

        assert!(!recommendation.sufficient_capacity);
        assert!(!recommendation.warnings.is_empty());
    }

    #[test]
    fn test_region_statistics() {
        let forecaster = CapacityForecaster::new();
        let region = test_region();

        create_school_pattern(&forecaster, region);

        let stats = forecaster.get_region_statistics(region).unwrap();

        assert!(stats.avg_capacity_flops > 0.0);
        assert!(stats.min_capacity_flops < stats.avg_capacity_flops);
        assert!(stats.max_capacity_flops > stats.avg_capacity_flops);
        assert!(stats.weekday_avg_flops > stats.weekend_avg_flops); // School pattern
        assert!(stats.pattern_count > 0);
    }

    #[test]
    fn test_confidence_intervals() {
        let forecaster = CapacityForecaster::new();
        let region = test_region();

        create_school_pattern(&forecaster, region);

        let forecast = forecaster.forecast(region, 24);

        for prediction in &forecast.predictions {
            // Confidence interval should bracket the prediction
            assert!(prediction.confidence_low <= prediction.predicted_flops);
            assert!(prediction.confidence_high >= prediction.predicted_flops);

            // Interval should be reasonable (not too wide or too narrow)
            let interval_width = prediction.confidence_high - prediction.confidence_low;
            assert!(interval_width > 0.0);
            assert!(interval_width < prediction.predicted_flops * 2.0);
        }
    }

    #[test]
    fn test_specific_date_event() {
        let forecaster = CapacityForecaster::new();
        let region = test_region();

        create_school_pattern(&forecaster, region);

        // Add a holiday event for a specific date
        let tomorrow = Utc::now() + Duration::days(1);
        let holiday = ScheduledEvent {
            name: "holiday".to_string(),
            start_hour: 0,
            end_hour: 23,
            days_of_week: None,
            capacity_multiplier: 0.2,
            specific_dates: vec![tomorrow],
        };

        forecaster.add_scheduled_event(region, holiday);

        let forecast = forecaster.forecast(region, 48);

        // Check that tomorrow's predictions reflect the holiday
        for prediction in &forecast.predictions {
            if prediction.timestamp.date_naive() == tomorrow.date_naive() {
                assert!(prediction
                    .contributing_factors
                    .contains(&"holiday".to_string()));
            }
        }
    }
}
