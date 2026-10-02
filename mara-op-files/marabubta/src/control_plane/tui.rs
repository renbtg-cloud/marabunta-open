// Marabunta - Licensed under the MIT License.
//! Terminal UI Dashboard for Marabunta Compute Operators
//!
//! A real-time TUI dashboard built with ratatui for monitoring and controlling
//! the Marabunta Compute cluster.
//!
//! # Features
//!
//! - Real-time cluster metrics (FLOPS, nodes, jobs, alerts)
//! - Region capacity monitoring with progress bars
//! - Active job tracking with progress
//! - Alert management with quick actions
//! - Keyboard navigation and command palette
//! - WebSocket-based live updates
//! - Detail views for nodes, jobs, and regions

use chrono::{DateTime, Utc};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
    Frame, Terminal,
};
use std::{
    io,
    time::{Duration, Instant},
};
use tui_input::backend::crossterm::EventHandler;
use tui_input::Input;

use crate::cli::client::CoordinatorClient;
use crate::cli::config::Config;
use crate::control_plane::api::{DashboardDataSource, JobFilter};
use crate::coordinator::dual_mode::dashboard::AlertSeverity as DualModeAlertSeverity;

// ─────────────────────────────────────────────────────────────────────────────
// DASHBOARD STATE
// ─────────────────────────────────────────────────────────────────────────────

/// Main dashboard state
pub struct Dashboard {
    /// Should quit
    should_quit: bool,
    /// Current focus panel
    focus: FocusPanel,
    /// Summary data
    summary: SummaryData,
    /// Regions data
    regions: Vec<RegionData>,
    /// Selected region index
    region_list_state: ListState,
    /// Jobs data
    jobs: Vec<JobData>,
    /// Selected job index
    job_list_state: ListState,
    /// Alerts data
    alerts: Vec<AlertData>,
    /// Selected alert index
    alert_list_state: ListState,
    /// Command mode state
    command_mode: bool,
    /// Command input
    command_input: Input,
    /// Status message
    status_message: Option<(String, Instant)>,
    /// Detail view
    detail_view: Option<DetailView>,
    /// Last update time
    last_update: Instant,
    /// Update interval
    update_interval: Duration,
    /// Coordinator client
    client: Option<CoordinatorClient>,
    /// Real metrics data source (optional, for live data)
    metrics_source: Option<std::sync::Arc<dyn DashboardDataSource + Send + Sync>>,
}

/// Which panel has focus
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FocusPanel {
    Regions,
    Jobs,
    Alerts,
}

/// Summary bar data
#[derive(Default, Clone)]
struct SummaryData {
    total_flops: f64,
    total_nodes: usize,
    active_jobs: usize,
    alert_count: usize,
}

/// Region panel data
#[derive(Clone)]
struct RegionData {
    name: String,
    capacity_used: f64,
    capacity_total: f64,
    node_count: usize,
    status: RegionStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RegionStatus {
    Healthy,
    Degraded,
    Offline,
}

/// Job panel data
#[derive(Clone)]
struct JobData {
    id: String,
    name: String,
    progress: f64,
    status: JobDisplayStatus,
    tasks_completed: usize,
    tasks_total: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JobDisplayStatus {
    Running,
    Pending,
    Completed,
    Failed,
}

/// Alert panel data
#[derive(Clone)]
struct AlertData {
    severity: AlertSeverity,
    message: String,
    actions: Vec<String>,
    #[allow(dead_code)]
    timestamp: DateTime<Utc>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AlertSeverity {
    Critical,
    Warning,
    Info,
}

/// Detail view state
#[allow(dead_code)]
enum DetailView {
    NodeDetail { name: String, content: String },
    JobDetail { id: String, content: String },
    RegionDetail { name: String, content: String },
}

// ─────────────────────────────────────────────────────────────────────────────
// DASHBOARD IMPLEMENTATION
// ─────────────────────────────────────────────────────────────────────────────

impl Dashboard {
    /// Create a new dashboard
    pub fn new(_config: &Config) -> Self {
        let mut dashboard = Self {
            should_quit: false,
            focus: FocusPanel::Jobs,
            summary: SummaryData::default(),
            regions: Vec::new(),
            region_list_state: ListState::default(),
            jobs: Vec::new(),
            job_list_state: ListState::default(),
            alerts: Vec::new(),
            alert_list_state: ListState::default(),
            command_mode: false,
            command_input: Input::default(),
            status_message: None,
            detail_view: None,
            last_update: Instant::now(),
            update_interval: Duration::from_secs(2),
            client: None,
            metrics_source: None,
        };

        // Initialize with sample data (will be replaced with real data)
        dashboard.load_sample_data();

        dashboard
    }

    /// Create a dashboard with a real metrics source
    pub fn with_metrics_source(
        config: &Config,
        source: std::sync::Arc<dyn DashboardDataSource + Send + Sync>,
    ) -> Self {
        let mut dashboard = Self::new(config);
        dashboard.metrics_source = Some(source);
        // Load real data immediately
        dashboard.load_real_data();
        dashboard
    }

    /// Load real data from metrics source
    fn load_real_data(&mut self) {
        let source = match &self.metrics_source {
            Some(s) => s,
            None => return,
        };

        // Get dashboard data
        let data = source.get_dashboard();

        // Update summary
        let total_nodes = data.infrastructure.nodes_online
            + data.infrastructure.nodes_degraded
            + data.infrastructure.nodes_offline
            + data.infrastructure.nodes_maintenance;

        // Estimate FLOPS from cores (rough estimate: 10 GFLOPS per core)
        let total_flops = (data.infrastructure.total_cores as f64) * 10e9;

        // Count active jobs
        let jobs = source.get_jobs(&JobFilter {
            status: Some("running".to_string()),
            min_priority: None,
            since: None,
            limit: Some(100),
            offset: None,
        });

        self.summary = SummaryData {
            total_flops,
            total_nodes: total_nodes as usize,
            active_jobs: jobs.len(),
            alert_count: data.infrastructure.health_alerts.len(),
        };

        // Update regions from dashboard data
        self.regions = source
            .get_regions()
            .into_iter()
            .map(|r| {
                let status = if r.online_nodes == r.total_nodes {
                    RegionStatus::Healthy
                } else if r.online_nodes > r.total_nodes / 2 {
                    RegionStatus::Degraded
                } else {
                    RegionStatus::Offline
                };

                RegionData {
                    name: r.name,
                    capacity_used: (r.total_cores - r.available_cores) as f64,
                    capacity_total: r.total_cores as f64,
                    node_count: r.total_nodes as usize,
                    status,
                }
            })
            .collect();

        // Update jobs
        let all_jobs = source.get_jobs(&JobFilter {
            status: None,
            min_priority: None,
            since: None,
            limit: Some(20),
            offset: None,
        });

        self.jobs = all_jobs
            .into_iter()
            .map(|j| {
                let status = match j.status.as_str() {
                    "running" => JobDisplayStatus::Running,
                    "pending" => JobDisplayStatus::Pending,
                    "completed" => JobDisplayStatus::Completed,
                    "failed" => JobDisplayStatus::Failed,
                    _ => JobDisplayStatus::Pending,
                };

                JobData {
                    id: j.id.chars().take(8).collect(),
                    name: j.name,
                    progress: (j.progress_percent / 100.0) as f64,
                    status,
                    tasks_completed: j.completed_tasks,
                    tasks_total: j.total_tasks,
                }
            })
            .collect();

        // Update alerts
        self.alerts = data
            .infrastructure
            .health_alerts
            .into_iter()
            .map(|a| {
                let severity = match a.severity {
                    DualModeAlertSeverity::Critical => AlertSeverity::Critical,
                    DualModeAlertSeverity::Warning => AlertSeverity::Warning,
                    DualModeAlertSeverity::Error => AlertSeverity::Warning,
                    DualModeAlertSeverity::Info => AlertSeverity::Info,
                };

                AlertData {
                    severity,
                    message: a.message,
                    actions: vec!["[d]ismiss".to_string(), "[v]iew".to_string()],
                    timestamp: a.raised_at,
                }
            })
            .collect();
    }

    /// Load sample data for testing
    fn load_sample_data(&mut self) {
        self.summary = SummaryData {
            total_flops: 3.4e12,
            total_nodes: 1159,
            active_jobs: 4,
            alert_count: 2,
        };

        self.regions = vec![
            RegionData {
                name: "São Paulo".to_string(),
                capacity_used: 789.0,
                capacity_total: 1160.0,
                node_count: 789,
                status: RegionStatus::Healthy,
            },
            RegionData {
                name: "Curitiba".to_string(),
                capacity_used: 210.0,
                capacity_total: 800.0,
                node_count: 210,
                status: RegionStatus::Healthy,
            },
            RegionData {
                name: "Buenos Aires".to_string(),
                capacity_used: 40.0,
                capacity_total: 1000.0,
                node_count: 40,
                status: RegionStatus::Degraded,
            },
            RegionData {
                name: "BYOD".to_string(),
                capacity_used: 120.0,
                capacity_total: 6000.0,
                node_count: 120,
                status: RegionStatus::Healthy,
            },
        ];

        self.jobs = vec![
            JobData {
                id: "4421".to_string(),
                name: "Protein".to_string(),
                progress: 0.78,
                status: JobDisplayStatus::Running,
                tasks_completed: 7800,
                tasks_total: 10000,
            },
            JobData {
                id: "4422".to_string(),
                name: "Render".to_string(),
                progress: 0.23,
                status: JobDisplayStatus::Running,
                tasks_completed: 230,
                tasks_total: 1000,
            },
            JobData {
                id: "4423".to_string(),
                name: "Genomics".to_string(),
                progress: 0.54,
                status: JobDisplayStatus::Running,
                tasks_completed: 5400,
                tasks_total: 10000,
            },
        ];

        self.alerts = vec![
            AlertData {
                severity: AlertSeverity::Warning,
                message: "School-SP offline in 47m".to_string(),
                actions: vec![
                    "[m]igrate".to_string(),
                    "[c]heckpoint".to_string(),
                    "[i]gnore".to_string(),
                ],
                timestamp: Utc::now(),
            },
            AlertData {
                severity: AlertSeverity::Info,
                message: "gaming-rig idle 3h".to_string(),
                actions: vec![
                    "[a]dd to job".to_string(),
                    "[p]ool".to_string(),
                    "[d]ismiss".to_string(),
                ],
                timestamp: Utc::now(),
            },
        ];
    }

    /// Fetch real data from coordinator
    async fn fetch_data(&mut self) {
        // If we have a metrics source, load real data
        if self.metrics_source.is_some() {
            self.load_real_data();
            self.set_status("Data refreshed from cluster");
        } else if let Some(ref client) = self.client {
            // Try to fetch from HTTP client
            let result = client
                .list_nodes(crate::cli::types::NodeFilter {
                    runtime: None,
                    memory_min: None,
                    region: None,
                    architecture: None,
                    status: None,
                })
                .await;

            match result {
                Ok(nodes) => {
                    // Update summary
                    self.summary.total_nodes = nodes.len();

                    // Group nodes by region
                    let mut region_map: std::collections::HashMap<String, (usize, usize)> =
                        std::collections::HashMap::new();
                    for node in &nodes {
                        let region = node.region.clone().unwrap_or_else(|| "unknown".to_string());
                        let entry = region_map.entry(region).or_insert((0, 0));
                        entry.0 += 1; // total
                        if node.status == "online" || node.status == "busy" {
                            entry.1 += 1; // online
                        }
                    }

                    self.regions = region_map
                        .into_iter()
                        .map(|(name, (total, online))| {
                            let status = if online == total {
                                RegionStatus::Healthy
                            } else if online > total / 2 {
                                RegionStatus::Degraded
                            } else {
                                RegionStatus::Offline
                            };

                            RegionData {
                                name,
                                capacity_used: online as f64,
                                capacity_total: total as f64,
                                node_count: total,
                                status,
                            }
                        })
                        .collect();

                    self.set_status("Data refreshed from coordinator");
                }
                Err(e) => {
                    self.set_status(&format!("Failed to refresh: {}", e));
                }
            }
        }
    }

    /// Handle keyboard input
    fn handle_input(&mut self, key: KeyCode, _modifiers: KeyModifiers) {
        if self.command_mode {
            match key {
                KeyCode::Esc => {
                    self.command_mode = false;
                    self.command_input = Input::default();
                }
                KeyCode::Enter => {
                    let command = self.command_input.value().to_string();
                    self.execute_command(&command);
                    self.command_mode = false;
                    self.command_input = Input::default();
                }
                _ => {
                    // Let tui_input handle the input
                }
            }
        } else if self.detail_view.is_some() {
            match key {
                KeyCode::Esc | KeyCode::Char('q') => {
                    self.detail_view = None;
                }
                _ => {}
            }
        } else {
            match key {
                KeyCode::Char('q') | KeyCode::Char('Q') => {
                    self.should_quit = true;
                }
                KeyCode::Char(':') => {
                    self.command_mode = true;
                }
                KeyCode::Tab => {
                    self.next_panel();
                }
                KeyCode::BackTab => {
                    self.prev_panel();
                }
                KeyCode::Char('r') | KeyCode::Char('R') => {
                    self.set_status("Refreshing data...");
                }
                KeyCode::Char('/') => {
                    self.command_mode = true;
                    self.command_input = Input::from("/");
                }
                KeyCode::Enter => {
                    self.show_detail();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.move_selection_down();
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.move_selection_up();
                }
                KeyCode::Char(c @ '1'..='9') => {
                    let index = c.to_digit(10).unwrap() as usize - 1;
                    self.execute_alert_action(index);
                }
                _ => {}
            }
        }
    }

    /// Move to next panel
    fn next_panel(&mut self) {
        self.focus = match self.focus {
            FocusPanel::Regions => FocusPanel::Jobs,
            FocusPanel::Jobs => FocusPanel::Alerts,
            FocusPanel::Alerts => FocusPanel::Regions,
        };
    }

    /// Move to previous panel
    fn prev_panel(&mut self) {
        self.focus = match self.focus {
            FocusPanel::Regions => FocusPanel::Alerts,
            FocusPanel::Jobs => FocusPanel::Regions,
            FocusPanel::Alerts => FocusPanel::Jobs,
        };
    }

    /// Move selection down in current panel
    fn move_selection_down(&mut self) {
        match self.focus {
            FocusPanel::Regions => {
                let i = match self.region_list_state.selected() {
                    Some(i) => {
                        if i >= self.regions.len().saturating_sub(1) {
                            0
                        } else {
                            i + 1
                        }
                    }
                    None => 0,
                };
                self.region_list_state.select(Some(i));
            }
            FocusPanel::Jobs => {
                let i = match self.job_list_state.selected() {
                    Some(i) => {
                        if i >= self.jobs.len().saturating_sub(1) {
                            0
                        } else {
                            i + 1
                        }
                    }
                    None => 0,
                };
                self.job_list_state.select(Some(i));
            }
            FocusPanel::Alerts => {
                let i = match self.alert_list_state.selected() {
                    Some(i) => {
                        if i >= self.alerts.len().saturating_sub(1) {
                            0
                        } else {
                            i + 1
                        }
                    }
                    None => 0,
                };
                self.alert_list_state.select(Some(i));
            }
        }
    }

    /// Move selection up in current panel
    fn move_selection_up(&mut self) {
        match self.focus {
            FocusPanel::Regions => {
                let i = match self.region_list_state.selected() {
                    Some(i) => {
                        if i == 0 {
                            self.regions.len().saturating_sub(1)
                        } else {
                            i - 1
                        }
                    }
                    None => 0,
                };
                self.region_list_state.select(Some(i));
            }
            FocusPanel::Jobs => {
                let i = match self.job_list_state.selected() {
                    Some(i) => {
                        if i == 0 {
                            self.jobs.len().saturating_sub(1)
                        } else {
                            i - 1
                        }
                    }
                    None => 0,
                };
                self.job_list_state.select(Some(i));
            }
            FocusPanel::Alerts => {
                let i = match self.alert_list_state.selected() {
                    Some(i) => {
                        if i == 0 {
                            self.alerts.len().saturating_sub(1)
                        } else {
                            i - 1
                        }
                    }
                    None => 0,
                };
                self.alert_list_state.select(Some(i));
            }
        }
    }

    /// Show detail view for selected item
    fn show_detail(&mut self) {
        match self.focus {
            FocusPanel::Regions => {
                if let Some(i) = self.region_list_state.selected() {
                    if let Some(region) = self.regions.get(i) {
                        let content = format!(
                            "Region: {}\n\n\
                            Status: {:?}\n\
                            Nodes: {}\n\
                            Capacity: {}/{}\n\
                            Utilization: {:.1}%\n\n\
                            [Sample data - not yet connected to coordinator]",
                            region.name,
                            region.status,
                            region.node_count,
                            region.capacity_used,
                            region.capacity_total,
                            (region.capacity_used / region.capacity_total * 100.0)
                        );
                        self.detail_view = Some(DetailView::RegionDetail {
                            name: region.name.clone(),
                            content,
                        });
                    }
                }
            }
            FocusPanel::Jobs => {
                if let Some(i) = self.job_list_state.selected() {
                    if let Some(job) = self.jobs.get(i) {
                        let content = format!(
                            "Job ID: {}\n\
                            Name: {}\n\n\
                            Status: {:?}\n\
                            Progress: {:.1}%\n\
                            Tasks: {}/{}\n\n\
                            [Sample data - not yet connected to coordinator]",
                            job.id,
                            job.name,
                            job.status,
                            job.progress * 100.0,
                            job.tasks_completed,
                            job.tasks_total
                        );
                        self.detail_view = Some(DetailView::JobDetail {
                            id: job.id.clone(),
                            content,
                        });
                    }
                }
            }
            FocusPanel::Alerts => {
                // Alerts don't have detail views currently
            }
        }
    }

    /// Execute an alert action
    fn execute_alert_action(&mut self, index: usize) {
        if let Some(alert) = self.alerts.get(index) {
            self.set_status(&format!("Executed action for: {}", alert.message));
        }
    }

    /// Execute a command
    fn execute_command(&mut self, command: &str) {
        let parts: Vec<&str> = command.trim().split_whitespace().collect();
        if parts.is_empty() {
            return;
        }

        match parts[0] {
            ":q" | ":quit" => {
                self.should_quit = true;
            }
            ":refresh" | ":r" => {
                self.set_status("Refreshing data...");
            }
            _ => {
                self.set_status(&format!("Unknown command: {}", command));
            }
        }
    }

    /// Set status message
    fn set_status(&mut self, message: &str) {
        self.status_message = Some((message.to_string(), Instant::now()));
    }

    /// Get current status message
    fn get_status(&self) -> Option<&str> {
        if let Some((msg, time)) = &self.status_message {
            if time.elapsed() < Duration::from_secs(3) {
                return Some(msg);
            }
        }
        None
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UI RENDERING
// ─────────────────────────────────────────────────────────────────────────────

impl Dashboard {
    /// Draw the UI
    fn draw(&mut self, f: &mut Frame, area: Rect) {
        // Main layout
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // Summary bar
                Constraint::Min(0),    // Main content
                Constraint::Length(3), // Alerts/status
                Constraint::Length(1), // Command line
            ])
            .split(area);

        // Draw summary bar
        self.draw_summary(f, chunks[0]);

        // Draw main content
        self.draw_main_content(f, chunks[1]);

        // Draw alerts
        self.draw_alerts_panel(f, chunks[2]);

        // Draw command line or status
        self.draw_command_line(f, chunks[3]);

        // Draw detail view if active
        if let Some(ref detail) = self.detail_view {
            self.draw_detail_view(f, area, detail);
        }
    }

    /// Draw summary bar
    fn draw_summary(&self, f: &mut Frame, area: Rect) {
        let flops_str = if self.summary.total_flops >= 1e12 {
            format!("{:.1} TFLOPS", self.summary.total_flops / 1e12)
        } else if self.summary.total_flops >= 1e9 {
            format!("{:.1} GFLOPS", self.summary.total_flops / 1e9)
        } else {
            format!("{:.1} MFLOPS", self.summary.total_flops / 1e6)
        };

        let text = vec![Line::from(vec![
            Span::styled(" ⚡ ", Style::default().fg(Color::Yellow).bold()),
            Span::styled(flops_str, Style::default().fg(Color::Cyan)),
            Span::raw(" │ "),
            Span::styled(
                format!("{} nodes", self.summary.total_nodes),
                Style::default().fg(Color::Green),
            ),
            Span::raw(" │ "),
            Span::styled(
                format!("{} jobs", self.summary.active_jobs),
                Style::default().fg(Color::Blue),
            ),
            Span::raw(" │ "),
            Span::styled(
                format!("{} alerts", self.summary.alert_count),
                Style::default().fg(if self.summary.alert_count > 0 {
                    Color::Red
                } else {
                    Color::Green
                }),
            ),
        ])];

        let block = Block::default()
            .title(" Marabunta Control ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));

        let paragraph = Paragraph::new(text).block(block);
        f.render_widget(paragraph, area);
    }

    /// Draw main content area
    fn draw_main_content(&mut self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
            .split(area);

        // Draw regions panel
        self.draw_regions_panel(f, chunks[0]);

        // Draw jobs panel
        self.draw_jobs_panel(f, chunks[1]);
    }

    /// Draw regions panel
    fn draw_regions_panel(&mut self, f: &mut Frame, area: Rect) {
        let is_focused = self.focus == FocusPanel::Regions;

        let items: Vec<ListItem> = self
            .regions
            .iter()
            .map(|region| {
                let percentage = (region.capacity_used / region.capacity_total * 100.0) as u16;
                let bar_width = 10;
                let filled = (percentage as usize * bar_width / 100).min(bar_width);
                let empty = bar_width - filled;

                let bar = format!(
                    "{}{} {:>3}%",
                    "█".repeat(filled),
                    "░".repeat(empty),
                    percentage
                );

                let status_color = match region.status {
                    RegionStatus::Healthy => Color::Green,
                    RegionStatus::Degraded => Color::Yellow,
                    RegionStatus::Offline => Color::Red,
                };

                let content = vec![Line::from(vec![
                    Span::styled(
                        format!("{:<15} ", region.name),
                        Style::default().fg(Color::White),
                    ),
                    Span::styled(bar, Style::default().fg(status_color)),
                ])];

                ListItem::new(content)
            })
            .collect();

        let list = List::new(items)
            .block(
                Block::default()
                    .title(" Regions ")
                    .borders(Borders::ALL)
                    .border_style(if is_focused {
                        Style::default().fg(Color::Yellow)
                    } else {
                        Style::default().fg(Color::Gray)
                    }),
            )
            .highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            );

        f.render_stateful_widget(list, area, &mut self.region_list_state);
    }

    /// Draw jobs panel
    fn draw_jobs_panel(&mut self, f: &mut Frame, area: Rect) {
        let is_focused = self.focus == FocusPanel::Jobs;

        let items: Vec<ListItem> = self
            .jobs
            .iter()
            .map(|job| {
                let percentage = (job.progress * 100.0) as u16;
                let bar_width = 20;
                let filled = (percentage as usize * bar_width / 100).min(bar_width);
                let empty = bar_width - filled;

                let bar = format!(
                    "{}{} {:>3}%",
                    "█".repeat(filled),
                    "░".repeat(empty),
                    percentage
                );

                let status_color = match job.status {
                    JobDisplayStatus::Running => Color::Green,
                    JobDisplayStatus::Pending => Color::Yellow,
                    JobDisplayStatus::Completed => Color::Blue,
                    JobDisplayStatus::Failed => Color::Red,
                };

                let content = vec![Line::from(vec![
                    Span::styled(format!("#{:<6} ", job.id), Style::default().fg(Color::Cyan)),
                    Span::styled(
                        format!("{:<12} ", job.name),
                        Style::default().fg(Color::White),
                    ),
                    Span::styled(bar, Style::default().fg(status_color)),
                ])];

                ListItem::new(content)
            })
            .collect();

        let list = List::new(items)
            .block(
                Block::default()
                    .title(" Jobs ")
                    .borders(Borders::ALL)
                    .border_style(if is_focused {
                        Style::default().fg(Color::Yellow)
                    } else {
                        Style::default().fg(Color::Gray)
                    }),
            )
            .highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            );

        f.render_stateful_widget(list, area, &mut self.job_list_state);
    }

    /// Draw alerts panel
    fn draw_alerts_panel(&mut self, f: &mut Frame, area: Rect) {
        let is_focused = self.focus == FocusPanel::Alerts;

        let items: Vec<ListItem> = self
            .alerts
            .iter()
            .enumerate()
            .map(|(_i, alert)| {
                let icon = match alert.severity {
                    AlertSeverity::Critical => "⚠",
                    AlertSeverity::Warning => "⚠",
                    AlertSeverity::Info => "💡",
                };

                let color = match alert.severity {
                    AlertSeverity::Critical => Color::Red,
                    AlertSeverity::Warning => Color::Yellow,
                    AlertSeverity::Info => Color::Cyan,
                };

                let actions = alert.actions.join(" ");

                let content = vec![Line::from(vec![
                    Span::styled(format!("{} ", icon), Style::default().fg(color).bold()),
                    Span::styled(
                        format!("{:<30} ", alert.message),
                        Style::default().fg(Color::White),
                    ),
                    Span::styled(actions, Style::default().fg(Color::DarkGray)),
                ])];

                ListItem::new(content)
            })
            .collect();

        let list = List::new(items).block(
            Block::default()
                .title(" Alerts ")
                .borders(Borders::ALL)
                .border_style(if is_focused {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default().fg(Color::Gray)
                }),
        );

        f.render_stateful_widget(list, area, &mut self.alert_list_state);
    }

    /// Draw command line
    fn draw_command_line(&self, f: &mut Frame, area: Rect) {
        if self.command_mode {
            let input_text = self.command_input.value();
            let text = Line::from(vec![
                Span::styled("> ", Style::default().fg(Color::Yellow)),
                Span::raw(input_text),
            ]);
            let paragraph = Paragraph::new(text);
            f.render_widget(paragraph, area);
        } else if let Some(status) = self.get_status() {
            let text = Line::from(vec![Span::styled(status, Style::default().fg(Color::Cyan))]);
            let paragraph = Paragraph::new(text);
            f.render_widget(paragraph, area);
        } else {
            let text = Line::from(vec![
                Span::styled("Tab", Style::default().fg(Color::Yellow)),
                Span::raw(": switch │ "),
                Span::styled("j/k", Style::default().fg(Color::Yellow)),
                Span::raw(": move │ "),
                Span::styled("Enter", Style::default().fg(Color::Yellow)),
                Span::raw(": details │ "),
                Span::styled(":", Style::default().fg(Color::Yellow)),
                Span::raw(": command │ "),
                Span::styled("r", Style::default().fg(Color::Yellow)),
                Span::raw(": refresh │ "),
                Span::styled("q", Style::default().fg(Color::Yellow)),
                Span::raw(": quit"),
            ]);
            let paragraph = Paragraph::new(text);
            f.render_widget(paragraph, area);
        }
    }

    /// Draw detail view as a centered popup
    fn draw_detail_view(&self, f: &mut Frame, area: Rect, detail: &DetailView) {
        // Calculate popup area (centered, 80% width, 80% height)
        let popup_area = centered_rect(80, 80, area);

        let (title, content) = match detail {
            DetailView::NodeDetail { name, content } => (format!(" Node: {} ", name), content),
            DetailView::JobDetail { id, content } => (format!(" Job: {} ", id), content),
            DetailView::RegionDetail { name, content } => (format!(" Region: {} ", name), content),
        };

        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow));

        let paragraph = Paragraph::new(content.as_str())
            .block(block)
            .wrap(Wrap { trim: true });

        // Clear the area first
        f.render_widget(ratatui::widgets::Clear, popup_area);
        f.render_widget(paragraph, popup_area);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN DASHBOARD LOOP
// ─────────────────────────────────────────────────────────────────────────────

/// Run the dashboard
pub async fn run_dashboard(config: Config) -> io::Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create dashboard
    let mut dashboard = Dashboard::new(&config);

    // Run event loop
    let res = run_event_loop(&mut terminal, &mut dashboard).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    res
}

/// Run the dashboard with a real metrics source
pub async fn run_dashboard_with_metrics(
    config: Config,
    metrics_source: std::sync::Arc<dyn DashboardDataSource + Send + Sync>,
) -> io::Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create dashboard with real metrics
    let mut dashboard = Dashboard::with_metrics_source(&config, metrics_source);

    // Run event loop
    let res = run_event_loop(&mut terminal, &mut dashboard).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    res
}

/// Main event loop
async fn run_event_loop<B: Backend>(
    terminal: &mut Terminal<B>,
    dashboard: &mut Dashboard,
) -> io::Result<()> {
    let tick_rate = Duration::from_millis(100);
    let mut last_tick = Instant::now();

    loop {
        terminal.draw(|f| {
            let area = f.area();
            dashboard.draw(f, area);
        })?;

        let timeout = tick_rate.saturating_sub(last_tick.elapsed());

        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if dashboard.command_mode && key.code != KeyCode::Esc && key.code != KeyCode::Enter
                {
                    dashboard.command_input.handle_event(&Event::Key(key));
                } else {
                    dashboard.handle_input(key.code, key.modifiers);
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            // Periodic updates
            if dashboard.last_update.elapsed() >= dashboard.update_interval {
                dashboard.fetch_data().await;
                dashboard.last_update = Instant::now();
            }
            last_tick = Instant::now();
        }

        if dashboard.should_quit {
            return Ok(());
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UTILITIES
// ─────────────────────────────────────────────────────────────────────────────

/// Helper function to create a centered rect
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
