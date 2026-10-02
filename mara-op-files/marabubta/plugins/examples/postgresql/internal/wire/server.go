// Marabunta - Licensed under the MIT License.
// Package wire implements the PostgreSQL wire protocol (pgwire) server.
// It accepts connections from standard PostgreSQL clients and translates
// them into distributed query execution via the parser, planner, and executor.
package wire

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marabunta/marabunta-postgres/internal/catalog"
	"github.com/marabunta/marabunta-postgres/internal/executor"
	"github.com/marabunta/marabunta-postgres/internal/parser"
	"github.com/marabunta/marabunta-postgres/internal/planner"
	"github.com/marabunta/marabunta-postgres/internal/txn"
)

// AuthConfig holds optional MD5 password authentication settings.
type AuthConfig struct {
	Enabled  bool
	Username string
	Password string
}

// Server is a pgwire-compatible TCP server.
type Server struct {
	addr     string
	listener net.Listener
	parser   *parser.Parser
	planner  *planner.Planner
	executor *executor.Executor
	catalog  *catalog.Catalog

	mu       sync.Mutex
	conns    map[uint64]*Conn
	nextID   uint64
	closed   atomic.Bool

	wg sync.WaitGroup

	// txnCoordinator is set when 2PC is enabled. It is passed to each
	// new connection for handling explicit BEGIN/COMMIT transactions.
	txnCoordinator *txn.TxnCoordinator

	// authConfig holds optional MD5 authentication settings.
	authConfig *AuthConfig

	// connTimeout is the maximum lifetime of a single connection.
	// Zero means no timeout.
	connTimeout time.Duration

	// stmtTimeout is the maximum execution time for a single statement.
	// Zero means no timeout.
	stmtTimeout time.Duration

	// connsByPID maps backend PID to connection for cancel request routing.
	connsByPID map[uint32]*Conn
}

// NewServer creates a new pgwire server.
func NewServer(addr string, p *parser.Parser, pl *planner.Planner, exec *executor.Executor, cat *catalog.Catalog) *Server {
	return &Server{
		addr:       addr,
		parser:     p,
		planner:    pl,
		executor:   exec,
		catalog:    cat,
		conns:      make(map[uint64]*Conn),
		connsByPID: make(map[uint32]*Conn),
	}
}

// SetTxnCoordinator sets the 2PC coordinator for the server. All new
// connections will receive a reference to this coordinator for handling
// explicit multi-shard transactions.
func (s *Server) SetTxnCoordinator(coord *txn.TxnCoordinator) {
	s.txnCoordinator = coord
}

// SetAuthConfig configures MD5 password authentication for the server.
// If nil or not enabled, connections proceed without authentication.
func (s *Server) SetAuthConfig(cfg *AuthConfig) {
	s.authConfig = cfg
}

// SetConnTimeout sets the per-connection lifetime timeout. Connections
// that exceed this duration are terminated. Zero means no timeout.
func (s *Server) SetConnTimeout(d time.Duration) {
	s.connTimeout = d
}

// SetStmtTimeout sets the per-statement execution timeout. Individual
// queries that exceed this duration are cancelled. Zero means no timeout.
func (s *Server) SetStmtTimeout(d time.Duration) {
	s.stmtTimeout = d
}

// cancelQuery cancels the running query on the connection identified by
// pid, if the secret key matches. This implements the PostgreSQL cancel
// request protocol (FE message code 80877102).
func (s *Server) cancelQuery(pid, secret uint32) {
	s.mu.Lock()
	conn, ok := s.connsByPID[pid]
	s.mu.Unlock()
	if ok && conn.secretKey == secret && conn.cancelFn != nil {
		conn.cancelFn()
	}
}

// ListenAndServe starts the pgwire listener and accepts connections.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln
	log.Printf("pgwire: listening on %s", s.addr)

	for {
		if s.closed.Load() {
			return nil
		}
		conn, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			log.Printf("pgwire: accept error: %v", err)
			continue
		}
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// Close gracefully shuts down the server.
func (s *Server) Close() {
	s.closed.Store(true)
	if s.listener != nil {
		s.listener.Close()
	}

	s.mu.Lock()
	for _, c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
}

func (s *Server) handleConnection(netConn net.Conn) {
	defer s.wg.Done()

	id := atomic.AddUint64(&s.nextID, 1)
	c := NewConn(id, netConn, s, s.parser, s.planner, s.executor, s.catalog)
	if s.txnCoordinator != nil {
		c.SetTxnCoordinator(s.txnCoordinator)
	}

	// Register connection by ID.
	s.mu.Lock()
	s.conns[id] = c
	s.mu.Unlock()

	// Register by PID for cancel request routing.
	pid := uint32(id)
	c.pid = pid
	s.mu.Lock()
	s.connsByPID[pid] = c
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.conns, id)
		delete(s.connsByPID, pid)
		s.mu.Unlock()
		c.Close()
	}()

	if err := c.Startup(); err != nil {
		if err != io.EOF {
			log.Printf("pgwire: conn %d startup error: %v", id, err)
		}
		return
	}

	// Create connection context with optional lifetime timeout.
	ctx := context.Background()
	var cancel context.CancelFunc
	if s.connTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.connTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// Store cancel function on the connection for query cancellation.
	c.cancelFn = cancel

	if err := c.Serve(ctx); err != nil {
		if err != io.EOF && !s.closed.Load() {
			log.Printf("pgwire: conn %d serve error: %v", id, err)
		}
	}
}
