// Marabunta - Licensed under the MIT License.
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Thermal severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum ThermalLevel {
    Normal = 0,   // < 60 C
    Elevated = 1, // 60-70 C
    High = 2,     // 70-80 C
    Critical = 3, // > 80 C
}

impl ThermalLevel {
    pub fn from_celsius(temp: f64) -> Self {
        if temp >= 80.0 {
            Self::Critical
        } else if temp >= 70.0 {
            Self::High
        } else if temp >= 60.0 {
            Self::Elevated
        } else {
            Self::Normal
        }
    }

    pub fn from_u8(v: u8) -> Self {
        match v {
            3.. => Self::Critical,
            2 => Self::High,
            1 => Self::Elevated,
            _ => Self::Normal,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Elevated => "elevated",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }
}

impl std::fmt::Display for ThermalLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub struct ThermalMonitor {
    level: Arc<AtomicU8>,
    temperature: Arc<std::sync::Mutex<Option<f64>>>,
    poll_interval: Duration,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl ThermalMonitor {
    pub fn new() -> Self {
        Self {
            level: Arc::new(AtomicU8::new(ThermalLevel::Normal as u8)),
            temperature: Arc::new(std::sync::Mutex::new(None)),
            poll_interval: Duration::from_secs(10),
            shutdown_tx: None,
        }
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn current_level(&self) -> ThermalLevel {
        ThermalLevel::from_u8(self.level.load(Ordering::Relaxed))
    }

    pub fn temperature_celsius(&self) -> Option<f64> {
        *self.temperature.lock().unwrap()
    }

    /// Start background monitoring loop
    pub fn start_monitoring(&mut self) {
        let (tx, mut rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(tx);

        let level = Arc::clone(&self.level);
        let temperature = Arc::clone(&self.temperature);
        let interval = self.poll_interval;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Some(temp) = read_temperature() {
                            let new_level = ThermalLevel::from_celsius(temp);
                            level.store(new_level as u8, Ordering::Relaxed);
                            *temperature.lock().unwrap() = Some(temp);
                        }
                    }
                    _ = rx.recv() => {
                        break;
                    }
                }
            }
        });
    }

    /// Stop monitoring
    pub fn stop(&mut self) {
        self.shutdown_tx.take(); // dropping sender closes channel
    }

    /// Set level manually (for Android JNI callback path)
    pub fn set_level(&self, level: ThermalLevel) {
        self.level.store(level as u8, Ordering::Relaxed);
    }
}

impl Default for ThermalMonitor {
    fn default() -> Self {
        Self::new()
    }
}

// ---- Platform-specific temperature reading ----

#[cfg(target_os = "linux")]
fn read_temperature() -> Option<f64> {
    // Read from sysfs thermal zones, return highest temperature
    let mut max_temp: Option<f64> = None;
    for i in 0..10 {
        let path = format!("/sys/class/thermal/thermal_zone{}/temp", i);
        if let Ok(content) = std::fs::read_to_string(&path) {
            if let Ok(millidegrees) = content.trim().parse::<f64>() {
                let celsius = millidegrees / 1000.0;
                max_temp = Some(max_temp.map_or(celsius, |m: f64| m.max(celsius)));
            }
        }
    }
    max_temp
}

#[cfg(any(target_os = "windows", target_os = "macos"))]
fn read_temperature() -> Option<f64> {
    use sysinfo::Components;
    let components = Components::new_with_refreshed_list();
    let mut max_temp: Option<f64> = None;
    for component in &components {
        let temp = component.temperature() as f64;
        if temp > 0.0 {
            max_temp = Some(max_temp.map_or(temp, |m: f64| m.max(temp)));
        }
    }
    max_temp
}

#[cfg(target_os = "android")]
fn read_temperature() -> Option<f64> {
    // Android uses JNI callback path (ThermalMonitor.java -> setThermalState)
    // The ThermalMonitor.set_level() method is called externally
    None
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "windows",
    target_os = "macos",
    target_os = "android"
)))]
fn read_temperature() -> Option<f64> {
    None
}

// ---- Tests ----
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thermal_level_from_celsius() {
        assert_eq!(ThermalLevel::from_celsius(45.0), ThermalLevel::Normal);
        assert_eq!(ThermalLevel::from_celsius(65.0), ThermalLevel::Elevated);
        assert_eq!(ThermalLevel::from_celsius(75.0), ThermalLevel::High);
        assert_eq!(ThermalLevel::from_celsius(85.0), ThermalLevel::Critical);
    }

    #[test]
    fn test_thermal_level_ordering() {
        assert!(ThermalLevel::Normal < ThermalLevel::Elevated);
        assert!(ThermalLevel::Elevated < ThermalLevel::High);
        assert!(ThermalLevel::High < ThermalLevel::Critical);
    }

    #[test]
    fn test_monitor_default_level() {
        let monitor = ThermalMonitor::new();
        assert_eq!(monitor.current_level(), ThermalLevel::Normal);
        assert_eq!(monitor.temperature_celsius(), None);
    }

    #[test]
    fn test_manual_level_set() {
        let monitor = ThermalMonitor::new();
        monitor.set_level(ThermalLevel::High);
        assert_eq!(monitor.current_level(), ThermalLevel::High);
    }

    #[test]
    fn test_thermal_level_display() {
        assert_eq!(ThermalLevel::Normal.as_str(), "normal");
        assert_eq!(ThermalLevel::Critical.as_str(), "critical");
    }
}
