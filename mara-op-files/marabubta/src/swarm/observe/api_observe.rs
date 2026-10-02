// Marabunta - Licensed under the MIT License.
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::Json;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::swarm::api::ApiState;
use crate::swarm::events::EventFilter;
use super::subscription::SubscriptionManager;
use super::types::*;
use super::views;

// ---- Subscription CRUD ----

#[derive(Deserialize)]
pub struct CreateSubscriptionRequest {
    pub filter: EventFilter,
}

#[derive(Serialize)]
pub struct SubscriptionResponse {
    pub id: String,
    pub created_at: String,
}

/// POST /api/v1/observe/subscriptions
pub async fn create_subscription(
    State(state): State<ApiState>,
    Json(req): Json<CreateSubscriptionRequest>,
) -> Result<(StatusCode, Json<SubscriptionResponse>), (StatusCode, Json<serde_json::Value>)> {
    let sub_mgr = require_oai(&state)?;
    let operator_id = resolve_operator();

    let handle = sub_mgr.create(operator_id, req.filter).map_err(|e| {
        oai_error_response(
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            &e.to_string(),
        )
    })?;

    Ok((
        StatusCode::CREATED,
        Json(SubscriptionResponse {
            id: handle.id.0.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
        }),
    ))
}

/// GET /api/v1/observe/subscriptions
pub async fn list_subscriptions(
    State(state): State<ApiState>,
) -> Result<Json<Vec<super::subscription::SubscriptionStats>>, (StatusCode, Json<serde_json::Value>)>
{
    let sub_mgr = require_oai(&state)?;
    Ok(Json(sub_mgr.list()))
}

/// DELETE /api/v1/observe/subscriptions/:id
pub async fn delete_subscription(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<serde_json::Value>)> {
    let sub_mgr = require_oai(&state)?;
    let sub_id = SubscriptionId(id);
    if sub_mgr.delete(&sub_id) {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(oai_error_response(
            StatusCode::NOT_FOUND,
            "subscription not found",
        ))
    }
}

/// GET /api/v1/observe/subscriptions/:id/stream (SSE)
///
/// Streams events matching the subscription's filter as Server-Sent Events.
pub async fn subscription_stream(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<
    Sse<impl Stream<Item = Result<SseEvent, std::convert::Infallible>>>,
    (StatusCode, Json<serde_json::Value>),
> {
    let sub_mgr = require_oai(&state)?;
    let sub_id = SubscriptionId(id);

    let event_bus = state
        .event_bus
        .as_ref()
        .ok_or_else(|| {
            oai_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "event bus not available",
            )
        })?;

    let sub_stats = sub_mgr
        .stats(&sub_id)
        .ok_or_else(|| {
            oai_error_response(StatusCode::NOT_FOUND, "subscription not found")
        })?;

    let filter = sub_stats.filter;
    let rx = event_bus.subscribe();

    // Use the same futures::stream::unfold pattern as the existing SSE endpoints
    let stream = futures::stream::unfold((rx, filter), |(mut rx, filter)| async move {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if event.matches_filter(&filter) {
                        let data = event.to_sse_data();
                        let sse_event = SseEvent::default().data(data);
                        return Some((Ok::<_, std::convert::Infallible>(sse_event), (rx, filter)));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new().interval(std::time::Duration::from_secs(15)),
    ))
}

// ---- Derived Views ----

/// GET /api/v1/observe/topology
pub async fn get_topology(
    State(state): State<ApiState>,
) -> Result<Json<views::TopologyView>, (StatusCode, Json<serde_json::Value>)> {
    let knowledge = &state.knowledge;
    let fleet = state
        .fleet_manager
        .as_ref()
        .ok_or_else(|| {
            oai_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "fleet manager not available",
            )
        })?;

    let topology = views::build_topology(knowledge, fleet);
    Ok(Json(topology))
}

/// GET /api/v1/observe/heatmap
pub async fn get_heatmap(
    State(state): State<ApiState>,
) -> Result<Json<views::ResourceHeatmap>, (StatusCode, Json<serde_json::Value>)> {
    let heatmap = views::build_resource_heatmap(&state.knowledge);
    Ok(Json(heatmap))
}

/// GET /api/v1/observe/admission
pub async fn get_admission_pipeline(
    State(state): State<ApiState>,
) -> Result<Json<views::AdmissionPipeline>, (StatusCode, Json<serde_json::Value>)> {
    let admission = state.admission_store.as_ref().ok_or_else(|| {
        oai_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "admission not available",
        )
    })?;

    let pipeline = views::build_admission_pipeline(admission);
    Ok(Json(pipeline))
}

// ---- Trace Queries ----

#[derive(Deserialize)]
pub struct TraceQuery {
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// GET /api/v1/observe/trace
pub async fn trace_events(
    State(state): State<ApiState>,
    Query(q): Query<TraceQuery>,
) -> Result<Json<Vec<serde_json::Value>>, (StatusCode, Json<serde_json::Value>)> {
    let event_bus = state.event_bus.as_ref().ok_or_else(|| {
        oai_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "event bus not available",
        )
    })?;

    let mut filter = EventFilter::default();
    if let Some(ref cid) = q.correlation_id {
        filter.correlation_id = Some(cid.clone());
    }

    let limit = q.limit.unwrap_or(100).min(1000);
    let events = event_bus.recent_filtered(&filter, limit);
    let json_events: Vec<serde_json::Value> = events
        .iter()
        .map(|e| serde_json::to_value(e).unwrap_or_default())
        .collect();

    Ok(Json(json_events))
}

// ---- Entity Inspection ----

#[derive(Deserialize)]
pub struct EntityQuery {
    pub at: Option<String>,
}

/// GET /api/v1/observe/entity/:type/:id
pub async fn inspect_entity(
    State(state): State<ApiState>,
    Path((entity_type, entity_id)): Path<(String, String)>,
    Query(q): Query<EntityQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    if let Some(ref at_str) = q.at {
        let at = chrono::DateTime::parse_from_rfc3339(at_str)
            .map_err(|_| {
                oai_error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid timestamp format (use RFC3339)",
                )
            })?
            .with_timezone(&chrono::Utc);

        let pg = state.pg_manager.as_ref().ok_or_else(|| {
            oai_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "PG not configured for time-travel",
            )
        })?;

        let entity_ref = EntityRef {
            entity_type: parse_entity_type(&entity_type).ok_or_else(|| {
                oai_error_response(StatusCode::BAD_REQUEST, "unknown entity type")
            })?,
            id: entity_id,
        };

        let result = super::pg::time_travel_entity(pg, &entity_ref, at)
            .await
            .map_err(|e| {
                oai_error_response(
                    StatusCode::from_u16(e.http_status())
                        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                    &e.to_string(),
                )
            })?;

        return Ok(Json(result));
    }

    // Live query from KnowledgeStore
    match entity_type.as_str() {
        "node" => {
            let node_id = Uuid::parse_str(&entity_id).map(crate::swarm::types::NodeId).map_err(|_| {
                oai_error_response(StatusCode::BAD_REQUEST, "invalid node UUID")
            })?;
            let node = state
                .knowledge
                .get_node(&node_id)
                .ok_or_else(|| oai_error_response(StatusCode::NOT_FOUND, "node not found"))?;
            Ok(Json(serde_json::to_value(&node).unwrap_or_default()))
        }
        "job" => {
            let job_id = Uuid::parse_str(&entity_id).map(crate::common::types::JobId).map_err(|_| {
                oai_error_response(StatusCode::BAD_REQUEST, "invalid job UUID")
            })?;
            let job = state
                .knowledge
                .get_job(&job_id)
                .ok_or_else(|| oai_error_response(StatusCode::NOT_FOUND, "job not found"))?;
            Ok(Json(serde_json::to_value(&job).unwrap_or_default()))
        }
        _ => Err(oai_error_response(
            StatusCode::BAD_REQUEST,
            "unsupported entity type for inspect",
        )),
    }
}

// ---- Helpers ----

fn require_oai(
    state: &ApiState,
) -> Result<&Arc<SubscriptionManager>, (StatusCode, Json<serde_json::Value>)> {
    state.subscription_manager.as_ref().ok_or_else(|| {
        oai_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "observe_and_interfere not enabled",
        )
    })
}

fn oai_error_response(
    status: StatusCode,
    message: &str,
) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({ "error": message })))
}

fn resolve_operator() -> OperatorId {
    OperatorId::new("default-operator")
}

pub fn parse_entity_type(s: &str) -> Option<EntityType> {
    match s {
        "node" => Some(EntityType::Node),
        "job" => Some(EntityType::Job),
        "chunk" => Some(EntityType::Chunk),
        "collective" => Some(EntityType::Collective),
        "plugin" => Some(EntityType::Plugin),
        "config" => Some(EntityType::Config),
        "marketplace_posting" => Some(EntityType::MarketplacePosting),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_entity_type_valid() {
        assert_eq!(parse_entity_type("node"), Some(EntityType::Node));
        assert_eq!(parse_entity_type("job"), Some(EntityType::Job));
        assert_eq!(parse_entity_type("config"), Some(EntityType::Config));
        assert_eq!(parse_entity_type("plugin"), Some(EntityType::Plugin));
    }

    #[test]
    fn test_parse_entity_type_invalid() {
        assert_eq!(parse_entity_type("unknown"), None);
        assert_eq!(parse_entity_type(""), None);
    }

    #[test]
    fn test_oai_error_response_format() {
        let (status, Json(body)) =
            oai_error_response(StatusCode::NOT_FOUND, "not found");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "not found");
    }

    #[test]
    fn test_subscription_response_serializes() {
        let resp = SubscriptionResponse {
            id: "sub-123".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
        };
        let json = serde_json::to_value(&resp).expect("serialize");
        assert_eq!(json["id"], "sub-123");
    }

    #[test]
    fn test_trace_query_defaults() {
        let q: TraceQuery = serde_json::from_value(serde_json::json!({})).expect("deserialize");
        assert!(q.correlation_id.is_none());
        assert!(q.limit.is_none());
    }

    #[test]
    fn test_entity_query_defaults() {
        let q: EntityQuery = serde_json::from_value(serde_json::json!({})).expect("deserialize");
        assert!(q.at.is_none());
    }
}
