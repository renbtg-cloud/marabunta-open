// Marabunta - Licensed under the MIT License.
//! Control plane - capacity forecasting, planning, and real-time monitoring

pub mod alerts;
pub mod api;
pub mod forecast;
pub mod metrics;
pub mod metrics_collector;
pub mod models;
pub mod tui;
pub mod web;
pub mod websocket;

pub use alerts::{
    ActionType, Alert as AlertDetection, AlertCondition, AlertDetector, AlertDetectorConfig,
    AlertEvent, AlertId, AlertRule, AlertStore, SchedulePattern, Severity, SuggestedAction,
    ThrottleSeverity, ThrottleType,
};
pub use api::{
    create_control_plane_router, create_control_plane_router_full,
    create_control_plane_router_with_metrics, create_control_plane_router_with_rate_limiting,
    default_control_plane_rate_limiter_config, AlertManager, AssignmentOverride,
    CapacityForecaster as CapacityForecasterTrait, ControlPlaneState, DashboardDataSource,
    InfraHealthDetails, JobMetricsResponse, NodeMetricsResponse, PhantomHealthDetails,
    SystemHealthResponse,
};
pub use forecast::{
    CapacityAnomaly, CapacityForecast as ForecastCapacityForecast, CapacityForecaster,
    HourlyPattern, HourlyPrediction as ForecastHourlyPrediction, JobPlanningRecommendation,
    RegionStatistics, ScheduledEvent,
};
pub use metrics::{
    FlopsEstimator, HealthScorer, LatencyPercentiles, MetricsAggregator, MetricsCollector,
    MetricsConfig, MetricsError, MetricsEvent, NodeAverages, NodeHealth, NodeReport,
    NodeReportBatch, ThrottlingInfo, ThrottlingReason, TimeSeriesStore, WorkUnitStats,
};
pub use metrics_collector::{
    CollectorConfig, JobHistoricalData, NodeHistoricalData, RealTimeMetricsCollector,
    RegionHistoricalData,
};
pub use models::*;
pub use tui::{run_dashboard, run_dashboard_with_metrics};
pub use web::create_dashboard_router;
pub use websocket::{
    broadcast_event, get_websocket_manager, init_websocket_manager, ClientMessage, DashboardEvent,
    NodeMetrics, SubscriptionFilter, WebSocketManager, WebSocketMetrics,
};
