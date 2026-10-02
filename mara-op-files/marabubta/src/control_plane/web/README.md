<!-- Marabunta - Licensed under the MIT License.
# Marabunta Compute Web Dashboard

A modern, real-time web dashboard for monitoring and managing your Marabunta Compute cluster. Built as a single self-contained HTML file with no build dependencies.

## Features

- **Real-time Updates**: WebSocket connection for live cluster metrics
- **Responsive Design**: Works on desktop, tablet, and mobile
- **Dark Theme**: Easy on the eyes with #1a1a1f background and #e07860 accents
- **No Dependencies**: Pure vanilla JavaScript, modern CSS, inline SVG

## Dashboard Sections

### 1. Overview Tab
- **Big Numbers**: Total TFLOPS, Active Nodes, Running Jobs
- **Network Topology**: SVG visualization showing coordinator and master hierarchy
- **Capacity Forecast**: 24-hour capacity prediction chart

### 2. Jobs Tab
- **Job Cards**: Progress bars, status indicators, metadata
- **Expandable Details**:
  - Latency heatmap by region
  - Work unit throughput sparklines
- **Real-time Progress**: Updates via WebSocket

### 3. Nodes Tab
- **Filterable Table**: Filter by region, pool, status
- **Sortable Columns**: Click headers to sort
- **Detail Modal**: Click row for comprehensive node information
- **Live Status**: Color-coded status indicators

### 4. Alerts Tab
- **Alert Cards**: Severity-based color coding (info, warning, error, critical)
- **Action Buttons**: Take action or dismiss alerts
- **Toast Notifications**: Pop-up notifications for new alerts

## Integration

### Basic Setup

```rust
use axum::Router;
use marabunta_compute::control_plane::web::create_dashboard_router;
use marabunta_compute::control_plane::websocket::WebSocketManager;

#[tokio::main]
async fn main() {
    // Create WebSocket manager
    let ws_manager = WebSocketManager::new(1000);

    // Build router
    let app = Router::new()
        .merge(create_dashboard_router())
        .merge(ws_manager.router());

    // Start server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

### Full Coordinator Integration

```rust
use std::sync::Arc;
use marabunta_compute::control_plane::web::create_complete_coordinator_router;
use marabunta_compute::coordinator::api::ApiState;
use marabunta_compute::coordinator::state::ClusterState;
use marabunta_compute::control_plane::websocket::{WebSocketManager, init_websocket_manager};

#[tokio::main]
async fn main() {
    // Initialize cluster state
    let cluster_state = Arc::new(ClusterState::new());

    // Create API state
    let api_state = Arc::new(ApiState {
        cluster: cluster_state.clone(),
    });

    // Initialize global WebSocket manager
    let ws_manager = init_websocket_manager();

    // Create complete router with API, dashboard, and WebSocket
    let app = create_complete_coordinator_router(api_state, ws_manager);

    // Start server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    println!("Dashboard: http://localhost:8080/dashboard");
    axum::serve(listener, app).await.unwrap();
}
```

## Broadcasting Events

The dashboard updates in real-time via WebSocket events:

```rust
use marabunta_compute::control_plane::websocket::{DashboardEvent, broadcast_event};
use chrono::Utc;

// Job progress update
broadcast_event(DashboardEvent::JobProgress {
    job_id: job.id.to_string(),
    completed: completed_tasks,
    total: total_tasks,
    throughput: tasks_per_second,
    timestamp: Utc::now(),
});

// Node status change
broadcast_event(DashboardEvent::NodeStatusChanged {
    node_id: worker.id.to_string(),
    old_status: "ready".to_string(),
    new_status: "busy".to_string(),
    timestamp: Utc::now(),
});

// Alert creation
use marabunta_compute::control_plane::websocket::{Alert, AlertSeverity};

broadcast_event(DashboardEvent::AlertCreated {
    alert: Alert {
        id: "alert-1".to_string(),
        severity: AlertSeverity::Warning,
        title: "High Latency Detected".to_string(),
        message: "Region us-west-2 latency: 250ms".to_string(),
        source: "latency_monitor".to_string(),
        created_at: Utc::now(),
    },
    timestamp: Utc::now(),
});
```

## WebSocket Protocol

The dashboard connects to `/ws/dashboard` with optional token authentication.

### Client Messages (Sent to Server)

```javascript
// Subscribe to specific jobs
ws.send(JSON.stringify({
  type: 'subscribe',
  filter: {
    jobs: ['job-12345678'],
    regions: ['region-abcd1234']
  }
}));

// Unsubscribe
ws.send(JSON.stringify({
  type: 'unsubscribe',
  filter: {
    jobs: ['job-12345678']
  }
}));
```

### Server Messages (Sent to Client)

```javascript
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);

  switch(data.type) {
    case 'node_status_changed':
      // data.node_id, data.old_status, data.new_status
      break;
    case 'job_progress':
      // data.job_id, data.completed, data.total, data.throughput
      break;
    case 'alert_created':
      // data.alert
      break;
    case 'heartbeat':
      // Keepalive ping
      break;
  }
};
```

## Demo Mode

If the WebSocket is not available, the dashboard automatically switches to demo mode after 2 seconds, displaying simulated data for testing and development.

## Endpoints

- `GET /dashboard` - Dashboard HTML page
- `WS /ws/dashboard?token=<auth-token>` - WebSocket endpoint for real-time updates
- `GET /cluster/status` - REST API for cluster statistics (used for initial load)

## Browser Requirements

- Modern browser with ES6+ support
- WebSocket support
- Canvas 2D API

Tested on:
- Chrome 90+
- Firefox 88+
- Safari 14+
- Edge 90+

## Customization

The dashboard is a single HTML file at `src/control_plane/web/dashboard.html`. You can customize:

- **Colors**: Edit CSS variables in `:root`
- **Layout**: Modify grid templates and flexbox
- **Charts**: Update canvas drawing functions
- **Data**: Change how data is fetched and displayed

## Security Considerations

1. **Authentication**: WebSocket requires token parameter (default: "default-token")
2. **HTTPS**: Use HTTPS in production for WSS (secure WebSocket)
3. **CORS**: Configure CORS headers for cross-origin access
4. **Rate Limiting**: Add rate limiting for WebSocket connections

## Performance

- **Lightweight**: <60KB total (HTML + CSS + JS)
- **Efficient**: WebSocket broadcasts only to subscribed clients
- **Scalable**: Supports 1000+ concurrent WebSocket connections
- **Smooth**: CSS transitions and requestAnimationFrame for animations

## Troubleshooting

### WebSocket won't connect
- Check that the server is running on the correct port
- Verify the auth token matches
- Check browser console for error messages

### Dashboard shows demo data
- WebSocket is not connected (by design)
- Server may not have WebSocket endpoint configured
- Authentication may have failed

### Charts not rendering
- Check browser console for JavaScript errors
- Verify canvas elements have proper dimensions
- Ensure data format matches expected structure

## License

Same as the main Marabunta Compute project.
