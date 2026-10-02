// Marabunta - Licensed under the MIT License.
package swarmclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// SwarmClient is a thread-safe client for communicating with the Marabunta
// Swarm host over the plugin wire protocol. It supports both Unix domain
// sockets and TCP connections.
//
// There are two operating modes:
//
//  1. Synchronous mode (default): Each public API method (Register, Store,
//     Fetch, etc.) performs a blocking request-response exchange.
//
//  2. Event-loop mode: Once StartEventLoop is called, a background goroutine
//     owns the read side of the connection. After calling StartEventLoop,
//     synchronous request-response methods MUST NOT be called because the
//     background reader would race with any synchronous reader. Only
//     write-only methods (Subscribe) remain safe.
type SwarmClient struct {
	mu       sync.Mutex
	conn     net.Conn
	pluginID string
	nodeID   string

	// EventCh receives asynchronous SubscribeEvent messages from the swarm.
	// The caller must drain this channel to prevent blocking the read loop.
	EventCh chan SubscribeEvent

	readTimeout  time.Duration
	writeTimeout time.Duration

	closed           bool
	eventLoopRunning bool
	done             chan struct{} // closed when Close() is called, signals event loop to exit

	// Reconnection fields. When maxRetries > 0 the client will attempt
	// exponential-backoff reconnection on connection loss instead of
	// permanently marking itself closed.
	address     string         // stored for reconnect (socket path or host:port)
	network     string         // "unix" or "tcp"
	maxRetries  int            // 0 = no reconnect (default, backward compatible)
	baseBackoff time.Duration  // initial backoff duration (e.g. 500ms)
	maxBackoff  time.Duration  // ceiling for exponential backoff (e.g. 30s)
	regReq      *RegisterRequest // saved registration for re-registration after reconnect
	reconnecting bool
	reconnectMu  sync.Mutex
}

// Option configures a SwarmClient.
type Option func(*SwarmClient)

// WithReadTimeout sets the read deadline for synchronous receive operations.
// This does NOT affect the event loop, which uses no deadline so it can wait
// indefinitely for incoming messages.
func WithReadTimeout(d time.Duration) Option {
	return func(c *SwarmClient) {
		c.readTimeout = d
	}
}

// WithWriteTimeout sets the write deadline for every send operation.
func WithWriteTimeout(d time.Duration) Option {
	return func(c *SwarmClient) {
		c.writeTimeout = d
	}
}

// WithEventBuffer sets the capacity of the async event channel.
func WithEventBuffer(size int) Option {
	return func(c *SwarmClient) {
		c.EventCh = make(chan SubscribeEvent, size)
	}
}

// WithReconnect enables exponential-backoff reconnection when the connection
// drops. maxRetries is the maximum number of reconnection attempts per
// disconnect event. baseBackoff is the initial sleep between attempts; it
// doubles on each failure up to a ceiling of 30 seconds.
//
// By default (maxRetries == 0) the client does not reconnect, preserving
// backward-compatible behavior.
func WithReconnect(maxRetries int, baseBackoff time.Duration) Option {
	return func(c *SwarmClient) {
		c.maxRetries = maxRetries
		c.baseBackoff = baseBackoff
		c.maxBackoff = 30 * time.Second
	}
}

// WithMaxBackoff overrides the maximum backoff ceiling set by WithReconnect.
// Only meaningful when WithReconnect is also used.
func WithMaxBackoff(d time.Duration) Option {
	return func(c *SwarmClient) {
		c.maxBackoff = d
	}
}

func newClient(conn net.Conn, network, address string, opts []Option) *SwarmClient {
	c := &SwarmClient{
		conn:         conn,
		network:      network,
		address:      address,
		EventCh:      make(chan SubscribeEvent, 256),
		readTimeout:  30 * time.Second,
		writeTimeout: 10 * time.Second,
		done:         make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Connect establishes a connection to the swarm host. The address is either
// a Unix socket path (starts with "/" or ".") or a TCP address ("host:port").
func Connect(address string, opts ...Option) (*SwarmClient, error) {
	network := "tcp"
	if len(address) > 0 && (address[0] == '/' || address[0] == '.') {
		network = "unix"
	}

	conn, err := net.DialTimeout(network, address, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect to %s/%s: %w", network, address, err)
	}
	return newClient(conn, network, address, opts), nil
}

// ConnectUnix connects via a Unix domain socket at the given path.
func ConnectUnix(path string, opts ...Option) (*SwarmClient, error) {
	conn, err := net.DialTimeout("unix", path, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect unix %s: %w", path, err)
	}
	return newClient(conn, "unix", path, opts), nil
}

// ConnectTCP connects via TCP to the given host:port.
func ConnectTCP(address string, opts ...Option) (*SwarmClient, error) {
	conn, err := net.DialTimeout("tcp", address, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect tcp %s: %w", address, err)
	}
	return newClient(conn, "tcp", address, opts), nil
}

// Close shuts down the connection. It is safe to call multiple times.
func (c *SwarmClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	close(c.done)
	// Close the connection first; this unblocks any pending ReadWireMessage
	// in the event loop goroutine, causing it to exit.
	err := c.conn.Close()
	close(c.EventCh)
	return err
}

// PluginID returns the ID assigned during registration, or empty if not yet registered.
func (c *SwarmClient) PluginID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pluginID
}

// NodeID returns this node's swarm ID, or empty if not yet registered.
func (c *SwarmClient) NodeID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodeID
}

// sendAndReceive sends a wire message and waits for the response.
// The caller MUST hold c.mu. This method must not be used after
// StartEventLoop has been called (the event loop owns the reader).
func (c *SwarmClient) sendAndReceive(reqType string, reqPayload interface{}) (*WireMessage, error) {
	if c.closed {
		return nil, fmt.Errorf("client is closed")
	}
	if c.eventLoopRunning {
		return nil, fmt.Errorf("cannot use synchronous request-response after StartEventLoop; event loop owns the reader")
	}

	msg := &WireMessage{
		Type:    reqType,
		Payload: reqPayload,
	}

	// Set write deadline.
	if c.writeTimeout > 0 {
		if err := c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			return nil, fmt.Errorf("set write deadline: %w", err)
		}
	}

	if err := WriteWireMessage(c.conn, msg); err != nil {
		return nil, fmt.Errorf("send %s: %w", reqType, err)
	}

	// Set read deadline.
	if c.readTimeout > 0 {
		if err := c.conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
			return nil, fmt.Errorf("set read deadline: %w", err)
		}
	}

	resp, err := ReadWireMessage(c.conn)
	if err != nil {
		return nil, fmt.Errorf("receive response for %s: %w", reqType, err)
	}

	// If we get an async SubscribeEvt while waiting for a response,
	// buffer it and keep reading until we get a non-event message.
	for resp.Type == TypeSubscribeEvt {
		var evt SubscribeEvent
		if decErr := DecodePayload(resp, &evt); decErr == nil {
			select {
			case c.EventCh <- evt:
			default:
				// Drop if channel full; caller should drain EventCh.
			}
		}
		if c.readTimeout > 0 {
			if err := c.conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
				return nil, fmt.Errorf("set read deadline: %w", err)
			}
		}
		resp, err = ReadWireMessage(c.conn)
		if err != nil {
			return nil, fmt.Errorf("receive response for %s (after event): %w", reqType, err)
		}
	}

	return resp, nil
}

// sendOnly sends a wire message without waiting for a response.
// The caller MUST hold c.mu. This is safe to use after StartEventLoop.
func (c *SwarmClient) sendOnly(msgType string, payload interface{}) error {
	if c.closed {
		return fmt.Errorf("client is closed")
	}

	msg := &WireMessage{
		Type:    msgType,
		Payload: payload,
	}

	if c.writeTimeout > 0 {
		if err := c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			return fmt.Errorf("set write deadline: %w", err)
		}
	}

	if err := WriteWireMessage(c.conn, msg); err != nil {
		return fmt.Errorf("send %s: %w", msgType, err)
	}
	return nil
}

// Register registers this plugin with the swarm and stores the assigned IDs.
// Must be called BEFORE StartEventLoop.
func (c *SwarmClient) Register(req RegisterRequest) (*RegisterResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Save the registration request so we can re-register after reconnect.
	c.regReq = &req

	resp, err := c.sendAndReceive(TypeRegisterReq, req)
	if err != nil {
		return nil, err
	}
	if resp.Type != TypeRegisterResp {
		return nil, fmt.Errorf("expected %s, got %s", TypeRegisterResp, resp.Type)
	}

	var out RegisterResponse
	if err := DecodePayload(resp, &out); err != nil {
		return nil, err
	}
	c.pluginID = out.PluginID
	c.nodeID = out.NodeID
	return &out, nil
}

// Store stores a key-value pair in the swarm.
// Must NOT be called after StartEventLoop.
func (c *SwarmClient) Store(req StoreRequest) (*StoreResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resp, err := c.sendAndReceive(TypeStoreReq, req)
	if err != nil {
		return nil, err
	}
	if resp.Type != TypeStoreResp {
		return nil, fmt.Errorf("expected %s, got %s", TypeStoreResp, resp.Type)
	}

	var out StoreResponse
	if err := DecodePayload(resp, &out); err != nil {
		return nil, err
	}
	if !out.Success && out.Error != "" {
		return &out, fmt.Errorf("store failed: %s", out.Error)
	}
	return &out, nil
}

// Fetch retrieves a value by key from the swarm.
// Must NOT be called after StartEventLoop.
func (c *SwarmClient) Fetch(req FetchRequest) (*FetchResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resp, err := c.sendAndReceive(TypeFetchReq, req)
	if err != nil {
		return nil, err
	}
	if resp.Type != TypeFetchResp {
		return nil, fmt.Errorf("expected %s, got %s", TypeFetchResp, resp.Type)
	}

	var out FetchResponse
	if err := DecodePayload(resp, &out); err != nil {
		return nil, err
	}
	if out.Error != "" {
		return &out, fmt.Errorf("fetch failed: %s", out.Error)
	}
	return &out, nil
}

// Delete deletes a key from the swarm.
// Must NOT be called after StartEventLoop.
func (c *SwarmClient) Delete(req DeleteRequest) (*DeleteResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resp, err := c.sendAndReceive(TypeDeleteReq, req)
	if err != nil {
		return nil, err
	}
	if resp.Type != TypeDeleteResp {
		return nil, fmt.Errorf("expected %s, got %s", TypeDeleteResp, resp.Type)
	}

	var out DeleteResponse
	if err := DecodePayload(resp, &out); err != nil {
		return nil, err
	}
	if !out.Success && out.Error != "" {
		return &out, fmt.Errorf("delete failed: %s", out.Error)
	}
	return &out, nil
}

// Scatter distributes work units across the swarm and gathers results.
// Must NOT be called after StartEventLoop.
func (c *SwarmClient) Scatter(req ScatterRequest) (*ScatterResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resp, err := c.sendAndReceive(TypeScatterReq, req)
	if err != nil {
		return nil, err
	}
	if resp.Type != TypeScatterResp {
		return nil, fmt.Errorf("expected %s, got %s", TypeScatterResp, resp.Type)
	}

	var out ScatterResponse
	if err := DecodePayload(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// FindNodes discovers swarm nodes matching the given criteria.
// Must NOT be called after StartEventLoop.
func (c *SwarmClient) FindNodes(req FindNodesRequest) (*FindNodesResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resp, err := c.sendAndReceive(TypeFindNodesReq, req)
	if err != nil {
		return nil, err
	}
	if resp.Type != TypeFindNodesResp {
		return nil, fmt.Errorf("expected %s, got %s", TypeFindNodesResp, resp.Type)
	}

	var out FindNodesResponse
	if err := DecodePayload(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Publish publishes a message to a topic.
// Must NOT be called after StartEventLoop.
func (c *SwarmClient) Publish(req PublishRequest) (*PublishResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resp, err := c.sendAndReceive(TypePublishReq, req)
	if err != nil {
		return nil, err
	}
	if resp.Type != TypePublishResp {
		return nil, fmt.Errorf("expected %s, got %s", TypePublishResp, resp.Type)
	}

	var out PublishResponse
	if err := DecodePayload(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Subscribe subscribes to a topic. Incoming events are delivered on EventCh.
// The swarm will send SubscribeEvt messages asynchronously after this call.
// This method is safe to call after StartEventLoop (it only writes).
func (c *SwarmClient) Subscribe(topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.sendOnly(TypeSubscribeReq, SubscribeRequest{Topic: topic})
}

// IsNodeAlive checks whether a specific node is considered alive by the swarm.
// Must NOT be called after StartEventLoop.
func (c *SwarmClient) IsNodeAlive(nodeID string) (*IsNodeAliveResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := IsNodeAliveRequest{NodeID: nodeID}
	resp, err := c.sendAndReceive(TypeIsNodeAliveReq, req)
	if err != nil {
		return nil, err
	}
	if resp.Type != TypeIsNodeAliveResp {
		return nil, fmt.Errorf("expected %s, got %s", TypeIsNodeAliveResp, resp.Type)
	}

	var out IsNodeAliveResponse
	if err := DecodePayload(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetNodeInfo retrieves this node's own information from the swarm.
// Must NOT be called after StartEventLoop.
func (c *SwarmClient) GetNodeInfo() (*GetNodeInfoResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resp, err := c.sendAndReceive(TypeGetNodeInfoReq, GetNodeInfoRequest{})
	if err != nil {
		return nil, err
	}
	if resp.Type != TypeGetNodeInfoResp {
		return nil, fmt.Errorf("expected %s, got %s", TypeGetNodeInfoResp, resp.Type)
	}

	var out GetNodeInfoResponse
	if err := DecodePayload(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// HealthCheck sends a health check request and returns the response.
// Must NOT be called after StartEventLoop.
func (c *SwarmClient) HealthCheck() (*HealthResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resp, err := c.sendAndReceive(TypeHealthReq, HealthRequest{})
	if err != nil {
		return nil, err
	}
	if resp.Type != TypeHealthResp {
		return nil, fmt.Errorf("expected %s, got %s", TypeHealthResp, resp.Type)
	}

	var out HealthResponse
	if err := DecodePayload(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// StoreString is a convenience wrapper that stores a string key-value pair
// with default options.
func (c *SwarmClient) StoreString(key, value string) (*StoreResponse, error) {
	return c.Store(StoreRequest{
		Key:     []byte(key),
		Value:   []byte(value),
		Options: DefaultStoreOptions(),
	})
}

// FetchString is a convenience wrapper that fetches a string value by key
// with default options.
func (c *SwarmClient) FetchString(key string) (string, bool, error) {
	resp, err := c.Fetch(FetchRequest{
		Key:     []byte(key),
		Options: DefaultFetchOptions(),
	})
	if err != nil {
		return "", false, err
	}
	if !resp.Found {
		return "", false, nil
	}
	return string(resp.Value), true, nil
}

// StoreJSON marshals v as JSON and stores it under the given key.
func (c *SwarmClient) StoreJSON(key string, v interface{}, opts StoreOptions) (*StoreResponse, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal value: %w", err)
	}
	return c.Store(StoreRequest{
		Key:     []byte(key),
		Value:   data,
		Options: opts,
	})
}

// FetchJSON fetches a key and unmarshals the JSON value into dst.
func (c *SwarmClient) FetchJSON(key string, dst interface{}, opts FetchOptions) (bool, error) {
	resp, err := c.Fetch(FetchRequest{
		Key:     []byte(key),
		Options: opts,
	})
	if err != nil {
		return false, err
	}
	if !resp.Found {
		return false, nil
	}
	if err := json.Unmarshal(resp.Value, dst); err != nil {
		return true, fmt.Errorf("unmarshal value: %w", err)
	}
	return true, nil
}

// RequestMigration requests data migration to a different node.
// Must NOT be called after StartEventLoop.
func (c *SwarmClient) RequestMigration(req MigrationRequest) (*MigrationResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resp, err := c.sendAndReceive(TypeMigrationReq, req)
	if err != nil {
		return nil, err
	}
	if resp.Type != TypeMigrationResp {
		return nil, fmt.Errorf("expected %s, got %s", TypeMigrationResp, resp.Type)
	}

	var out MigrationResponse
	if err := DecodePayload(resp, &out); err != nil {
		return nil, err
	}
	if !out.Accepted && out.Error != "" {
		return &out, fmt.Errorf("migration rejected: %s", out.Error)
	}
	return &out, nil
}

// reconnect attempts to re-establish the connection using exponential backoff.
// If the client was previously registered, it re-registers automatically.
// Returns nil on success or an error if all attempts are exhausted.
func (c *SwarmClient) reconnect() error {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	if c.reconnecting {
		return errors.New("reconnect already in progress")
	}
	c.reconnecting = true
	defer func() { c.reconnecting = false }()

	backoff := c.baseBackoff
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		// Sleep before each attempt (including the first) to avoid
		// hammering the server immediately after a disconnect.
		time.Sleep(backoff)

		conn, err := net.DialTimeout(c.network, c.address, 10*time.Second)
		if err != nil {
			backoff = min(backoff*2, c.maxBackoff)
			log.Printf("swarmclient: reconnect attempt %d/%d failed: %v", attempt+1, c.maxRetries, err)
			continue
		}

		c.mu.Lock()
		c.conn = conn
		c.closed = false
		c.mu.Unlock()

		// Re-register if we had a prior registration.
		if c.regReq != nil {
			regResp, regErr := c.reregister(*c.regReq)
			if regErr != nil {
				log.Printf("swarmclient: re-registration failed on attempt %d: %v", attempt+1, regErr)
				conn.Close()
				c.mu.Lock()
				c.closed = true
				c.mu.Unlock()
				backoff = min(backoff*2, c.maxBackoff)
				continue
			}
			c.mu.Lock()
			c.pluginID = regResp.PluginID
			c.nodeID = regResp.NodeID
			c.mu.Unlock()
		}

		log.Printf("swarmclient: reconnected after %d attempt(s)", attempt+1)
		return nil
	}
	return fmt.Errorf("reconnect failed after %d attempts", c.maxRetries)
}

// reregister performs a register exchange on the current connection without
// acquiring c.mu (the caller is responsible for ensuring the connection is
// valid). This avoids a deadlock in reconnect() which already manipulates
// c.mu around the conn swap.
func (c *SwarmClient) reregister(req RegisterRequest) (*RegisterResponse, error) {
	msg := &WireMessage{
		Type:    TypeRegisterReq,
		Payload: req,
	}

	if c.writeTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if err := WriteWireMessage(c.conn, msg); err != nil {
		return nil, fmt.Errorf("send RegisterReq: %w", err)
	}

	if c.readTimeout > 0 {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}
	resp, err := ReadWireMessage(c.conn)
	if err != nil {
		return nil, fmt.Errorf("receive RegisterResp: %w", err)
	}
	if resp.Type != TypeRegisterResp {
		return nil, fmt.Errorf("expected %s, got %s", TypeRegisterResp, resp.Type)
	}

	var out RegisterResponse
	if err := DecodePayload(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// startHealthPing launches a background goroutine that periodically checks
// the connection's liveness. If a write-deadline probe fails and reconnection
// is enabled, it triggers a reconnect attempt.
func (c *SwarmClient) startHealthPing(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-c.done:
				return
			case <-ticker.C:
				c.mu.Lock()
				if c.closed {
					c.mu.Unlock()
					if c.maxRetries > 0 {
						if err := c.reconnect(); err != nil {
							log.Printf("swarmclient: health ping reconnect failed: %v", err)
						}
					}
					continue
				}
				// Set a short write deadline to detect stale connections.
				// We don't actually send a ping message (the protocol has no
				// ping frame), but the deadline check on the next real write
				// will surface a broken pipe.
				_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				c.mu.Unlock()
			}
		}
	}()
}

// StartEventLoop launches a background goroutine that continuously reads
// messages from the swarm connection. SubscribeEvt messages are forwarded
// to EventCh; other messages are passed to the handler callback.
//
// The loop exits when the connection is closed or an unrecoverable error
// occurs. Use this when the plugin needs to handle both async events and
// swarm-initiated requests (Start, Stop, Handle, Health).
//
// IMPORTANT: After calling StartEventLoop, synchronous request-response
// methods (Store, Fetch, Delete, Scatter, FindNodes, Publish, IsNodeAlive,
// GetNodeInfo, HealthCheck) must NOT be called. Only Subscribe (write-only)
// remains safe. The event loop goroutine is the sole reader on the connection.
func (c *SwarmClient) StartEventLoop(handler func(msg *WireMessage) *WireMessage) {
	c.mu.Lock()
	c.eventLoopRunning = true
	c.mu.Unlock()

	go func() {
		for {
			// The event loop does NOT set a read deadline. It waits
			// indefinitely for incoming messages. The loop exits when
			// the connection is closed (e.g., via Close()).
			_ = c.conn.SetReadDeadline(time.Time{}) // clear any prior deadline

			msg, err := ReadWireMessage(c.conn)
			if err != nil {
				// Connection closed or fatal error.
				// Check if this is an expected shutdown.
				select {
				case <-c.done:
					// Clean shutdown, don't log.
					return
				default:
					if c.maxRetries > 0 {
						log.Printf("swarmclient: connection lost, attempting reconnect: %v", err)
						if reconnErr := c.reconnect(); reconnErr != nil {
							log.Printf("swarmclient: reconnect failed: %v", reconnErr)
							return
						}
						continue // restart the read loop on the new connection
					}
					log.Printf("swarmclient: event loop read error: %v", err)
					return
				}
			}

			switch msg.Type {
			case TypeSubscribeEvt:
				var evt SubscribeEvent
				if decErr := DecodePayload(msg, &evt); decErr == nil {
					select {
					case c.EventCh <- evt:
					default:
						log.Printf("swarmclient: event channel full, dropping event on topic %q", evt.Topic)
					}
				}
			default:
				if handler != nil {
					resp := handler(msg)
					if resp != nil {
						c.mu.Lock()
						if c.writeTimeout > 0 {
							_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
						}
						if err := WriteWireMessage(c.conn, resp); err != nil {
							log.Printf("swarmclient: event loop write error: %v", err)
							c.mu.Unlock()
							return
						}
						c.mu.Unlock()
					}
				}
			}
		}
	}()
}
