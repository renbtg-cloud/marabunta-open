// Marabunta - Licensed under the MIT License.
package wire

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/marabunta/marabunta-postgres/internal/catalog"
	"github.com/marabunta/marabunta-postgres/internal/executor"
	"github.com/marabunta/marabunta-postgres/internal/parser"
	"github.com/marabunta/marabunta-postgres/internal/planner"
	"github.com/marabunta/marabunta-postgres/internal/plpgsql"
	"github.com/marabunta/marabunta-postgres/internal/txn"
)

// pgwire message type bytes.
const (
	msgQuery       byte = 'Q'
	msgParse       byte = 'P'
	msgBind        byte = 'B'
	msgDescribe    byte = 'D'
	msgExecute     byte = 'E'
	msgSync        byte = 'S'
	msgTerminate   byte = 'X'
	msgClose       byte = 'C'
	msgFlush       byte = 'H'

	// Server -> Client
	msgAuth            byte = 'R'
	msgParamStatus     byte = 'S'
	msgBackendKeyData  byte = 'K'
	msgReadyForQuery   byte = 'Z'
	msgRowDescription  byte = 'T'
	msgDataRow         byte = 'D'
	msgCommandComplete byte = 'C'
	msgErrorResponse   byte = 'E'
	msgEmptyQuery      byte = 'I'
	msgParseComplete   byte = '1'
	msgBindComplete    byte = '2'
	msgCloseComplete   byte = '3'
	msgNoData          byte = 'n'
)

// BufferedOp represents a write operation buffered within a client-side
// explicit transaction (BEGIN ... COMMIT). When the client issues COMMIT,
// all buffered operations are executed atomically via 2PC.
type BufferedOp struct {
	Table   string
	ShardID int
	SQL     string
}

// Conn represents a single pgwire client connection.
type Conn struct {
	id       uint64
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	server   *Server
	parser   *parser.Parser
	planner  *planner.Planner
	executor *executor.Executor
	catalog  *catalog.Catalog

	params         map[string]string
	preparedStmts  map[string]string // name -> SQL query
	portalBindings map[string]string // portal -> statement name

	// Transaction state for explicit client transactions (BEGIN/COMMIT/ROLLBACK).
	inTxn       bool
	txnID       string
	bufferedOps []BufferedOp
	txnFailed   bool // true if any statement in the txn block failed

	// txnCoordinator is the 2PC coordinator for multi-shard commits.
	txnCoordinator *txn.TxnCoordinator

	// pid is the backend process ID sent to the client in BackendKeyData.
	pid uint32

	// secretKey is the random secret sent with BackendKeyData, used to
	// validate cancel requests.
	secretKey uint32

	// cancelFn cancels the connection's context, used by the cancel protocol.
	cancelFn context.CancelFunc
}

// NewConn creates a new connection handler.
func NewConn(id uint64, netConn net.Conn, srv *Server, p *parser.Parser, pl *planner.Planner, exec *executor.Executor, cat *catalog.Catalog) *Conn {
	// Generate a random secret key for cancel request validation.
	var secretBuf [4]byte
	_, _ = rand.Read(secretBuf[:])
	secret := binary.BigEndian.Uint32(secretBuf[:])

	return &Conn{
		id:        id,
		conn:      netConn,
		reader:    bufio.NewReaderSize(netConn, 8192),
		writer:    bufio.NewWriterSize(netConn, 8192),
		server:    srv,
		parser:    p,
		planner:   pl,
		executor:  exec,
		catalog:   cat,
		params:    make(map[string]string),
		secretKey: secret,
	}
}

// SetTxnCoordinator sets the 2PC coordinator for handling explicit
// multi-shard transactions initiated by BEGIN/COMMIT.
func (c *Conn) SetTxnCoordinator(coord *txn.TxnCoordinator) {
	c.txnCoordinator = coord
}

// Close closes the connection.
func (c *Conn) Close() {
	c.conn.Close()
}

// authenticate performs MD5 password authentication if the server has an
// AuthConfig. If auth is disabled or not configured, it sends AuthOk
// immediately (backward compatible).
func (c *Conn) authenticate(user string) error {
	cfg := c.server.authConfig
	if cfg == nil || !cfg.Enabled {
		c.writeAuthOk()
		return nil
	}

	// Generate a random 4-byte salt.
	var salt [4]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return fmt.Errorf("generate salt: %w", err)
	}

	// Send AuthenticationMD5Password request.
	c.writeAuthMD5Password(salt[:])
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("flush auth request: %w", err)
	}

	// Read the client's PasswordMessage response.
	clientHash, err := c.readPasswordMessage()
	if err != nil {
		return fmt.Errorf("read password: %w", err)
	}

	// Compute expected hash: md5(md5(password + user) + salt).
	// Step 1: inner = md5(password + user) as hex string.
	inner := md5.Sum([]byte(cfg.Password + user))
	innerHex := hex.EncodeToString(inner[:])
	// Step 2: outer = md5(innerHex + salt) as hex string, prefixed with "md5".
	outer := md5.Sum(append([]byte(innerHex), salt[:]...))
	expected := "md5" + hex.EncodeToString(outer[:])

	if clientHash != expected {
		c.writeError("FATAL", "28P01", "password authentication failed for user \""+user+"\"")
		c.writer.Flush()
		return fmt.Errorf("md5 auth failed for user %q", user)
	}

	c.writeAuthOk()
	return nil
}

// Startup handles the PostgreSQL startup handshake.
func (c *Conn) Startup() error {
	// Read startup message length.
	var length int32
	if err := binary.Read(c.reader, binary.BigEndian, &length); err != nil {
		return fmt.Errorf("read startup length: %w", err)
	}
	if length < 4 || length > 10000 {
		return fmt.Errorf("invalid startup length: %d", length)
	}

	body := make([]byte, length-4)
	if _, err := io.ReadFull(c.reader, body); err != nil {
		return fmt.Errorf("read startup body: %w", err)
	}

	// Check protocol version.
	if len(body) < 4 {
		return fmt.Errorf("startup body too short")
	}
	major := binary.BigEndian.Uint16(body[0:2])
	minor := binary.BigEndian.Uint16(body[2:4])

	// SSL request (80877103).
	if major == 1234 && minor == 5679 {
		// Reject SSL with 'N'.
		if _, err := c.conn.Write([]byte{'N'}); err != nil {
			return err
		}
		// Client will retry with normal startup.
		return c.Startup()
	}

	// Cancel request (80877102).
	if major == 1234 && minor == 5678 {
		if len(body) >= 12 {
			pid := binary.BigEndian.Uint32(body[4:8])
			secret := binary.BigEndian.Uint32(body[8:12])
			c.server.cancelQuery(pid, secret)
		}
		return nil
	}

	// Parse startup parameters (null-terminated key-value pairs).
	params := body[4:]
	for len(params) > 1 {
		keyEnd := indexOf(params, 0)
		if keyEnd < 0 {
			break
		}
		key := string(params[:keyEnd])
		params = params[keyEnd+1:]

		valEnd := indexOf(params, 0)
		if valEnd < 0 {
			break
		}
		val := string(params[:valEnd])
		params = params[valEnd+1:]

		c.params[key] = val
	}

	log.Printf("pgwire: conn %d startup user=%s database=%s", c.id, c.params["user"], c.params["database"])

	// Authenticate the client.
	if err := c.authenticate(c.params["user"]); err != nil {
		return fmt.Errorf("authentication failed: %w", err)
	}

	// Send parameter status messages.
	c.writeParamStatus("server_version", "15.0.0-marabunta")
	c.writeParamStatus("server_encoding", "UTF8")
	c.writeParamStatus("client_encoding", "UTF8")
	c.writeParamStatus("DateStyle", "ISO, MDY")
	c.writeParamStatus("integer_datetimes", "on")

	// Send BackendKeyData with PID and secret for cancel support.
	c.writeBackendKeyData(int32(c.pid), int32(c.secretKey))

	// Send ReadyForQuery.
	c.writeReadyForQuery('I')

	return c.writer.Flush()
}

// Serve enters the main command loop, processing client messages.
func (c *Conn) Serve(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgType, body, err := c.readMessage()
		if err != nil {
			return err
		}

		switch msgType {
		case msgQuery:
			if err := c.handleSimpleQuery(ctx, body); err != nil {
				log.Printf("pgwire: conn %d query error: %v", c.id, err)
			}
		case msgParse:
			c.handleParse(body)
		case msgBind:
			c.handleBind(body)
		case msgDescribe:
			c.handleDescribe(body)
		case msgExecute:
			if err := c.handleExecute(ctx, body); err != nil {
				log.Printf("pgwire: conn %d execute error: %v", c.id, err)
			}
		case msgSync:
			c.writeReadyForQuery(c.txnStatusByte())
			c.writer.Flush()
		case msgClose:
			c.writeCloseComplete()
			c.writer.Flush()
		case msgFlush:
			c.writer.Flush()
		case msgTerminate:
			return io.EOF
		default:
			log.Printf("pgwire: conn %d unknown message type %c", c.id, msgType)
		}
	}
}

// maxMessageLength is the maximum allowed pgwire message length (64 MB).
const maxMessageLength = 64 * 1024 * 1024

func (c *Conn) readMessage() (byte, []byte, error) {
	msgType, err := c.reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	var length int32
	if err := binary.Read(c.reader, binary.BigEndian, &length); err != nil {
		return 0, nil, err
	}
	if length < 4 {
		return 0, nil, fmt.Errorf("invalid message length: %d", length)
	}
	if length > maxMessageLength {
		return 0, nil, fmt.Errorf("message too large: %d bytes (max %d)", length, maxMessageLength)
	}

	body := make([]byte, length-4)
	if _, err := io.ReadFull(c.reader, body); err != nil {
		return 0, nil, err
	}
	return msgType, body, nil
}

func (c *Conn) handleSimpleQuery(ctx context.Context, body []byte) error {
	// Query string is null-terminated.
	query := strings.TrimRight(string(body), "\x00")
	if query == "" {
		c.writeEmptyQuery()
		c.writeReadyForQuery(c.txnStatusByte())
		return c.writer.Flush()
	}

	// Parse SQL.
	stmts, err := c.parser.Parse(query)
	if err != nil {
		c.writeError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
		if c.inTxn {
			c.txnFailed = true
		}
		c.writeReadyForQuery(c.txnStatusByte())
		return c.writer.Flush()
	}

	for _, stmt := range stmts {
		// Handle transaction control statements.
		switch stmt.Type {
		case parser.StmtBegin:
			c.handleBegin()
			c.writeCommandComplete("BEGIN")
			continue
		case parser.StmtCommit:
			c.handleCommit(ctx)
			continue
		case parser.StmtRollback:
			c.handleRollback(ctx)
			c.writeCommandComplete("ROLLBACK")
			continue
		}

		// If we are in a failed transaction block, reject everything except
		// ROLLBACK (already handled above).
		if c.inTxn && c.txnFailed {
			c.writeError("ERROR", "25P02", "current transaction is aborted, commands ignored until end of transaction block")
			continue
		}

		// Handle PL/pgSQL statements: CREATE FUNCTION/PROCEDURE, DO, CALL.
		switch stmt.Type {
		case parser.StmtCreateFunction:
			c.handleCreateFunction(ctx, stmt)
			continue
		case parser.StmtDO:
			c.handleDO(ctx, stmt)
			continue
		case parser.StmtCall:
			c.handleCall(ctx, stmt)
			continue
		}

		// Plan the query.
		plan, err := c.planner.Plan(stmt)
		if err != nil {
			c.writeError("ERROR", "42000", fmt.Sprintf("planning error: %v", err))
			if c.inTxn {
				c.txnFailed = true
			}
			continue
		}

		// If inside an explicit transaction and this is a write, buffer it
		// for 2PC on COMMIT instead of executing immediately.
		if c.inTxn && c.txnCoordinator != nil && isWriteStatement(stmt) {
			c.bufferWrite(plan, stmt)
			continue
		}

		// Execute with optional per-statement timeout.
		execCtx := ctx
		if c.server.stmtTimeout > 0 {
			var stmtCancel context.CancelFunc
			execCtx, stmtCancel = context.WithTimeout(ctx, c.server.stmtTimeout)
			defer stmtCancel()
		}
		result, err := c.executor.Execute(execCtx, plan)
		if err != nil {
			c.writeError("ERROR", "XX000", fmt.Sprintf("execution error: %v", err))
			if c.inTxn {
				c.txnFailed = true
			}
			continue
		}

		// Send results.
		if result.Columns != nil && len(result.Columns) > 0 {
			c.writeRowDescription(result.Columns)
			numCols := len(result.Columns)
			// Rows is a flat [][]byte — each group of numCols values forms one row.
			for i := 0; i+numCols <= len(result.Rows); i += numCols {
				c.writeDataRow(result.Rows[i : i+numCols])
			}
		}
		c.writeCommandComplete(result.Tag)
	}

	c.writeReadyForQuery(c.txnStatusByte())
	return c.writer.Flush()
}

// handleBegin starts a new explicit client transaction.
func (c *Conn) handleBegin() {
	c.inTxn = true
	c.txnFailed = false
	c.bufferedOps = nil
	c.txnID = "" // Will be assigned by coordinator on commit.
	log.Printf("pgwire: conn %d BEGIN transaction", c.id)
}

// handleCommit commits an explicit client transaction. If there are buffered
// multi-shard writes, they are executed atomically via 2PC.
func (c *Conn) handleCommit(ctx context.Context) {
	if !c.inTxn {
		c.writeError("WARNING", "25P01", "there is no transaction in progress")
		c.writeCommandComplete("COMMIT")
		return
	}

	if c.txnFailed {
		// Transaction was already aborted due to an error.
		c.writeError("ERROR", "25P02", "current transaction is aborted, COMMIT treated as ROLLBACK")
		c.resetTxnState()
		return
	}

	// If there are buffered writes, execute them via 2PC.
	if len(c.bufferedOps) > 0 && c.txnCoordinator != nil {
		targets := make([]txn.ShardTarget, len(c.bufferedOps))
		for i, op := range c.bufferedOps {
			targets[i] = txn.ShardTarget{
				Table:   op.Table,
				ShardID: op.ShardID,
				SQL:     op.SQL,
			}
		}

		if err := c.txnCoordinator.ExecuteDistributed(ctx, targets); err != nil {
			c.writeError("ERROR", "XX000", fmt.Sprintf("2PC commit failed: %v", err))
			c.resetTxnState()
			return
		}
	}

	numOps := len(c.bufferedOps)
	c.resetTxnState()
	c.writeCommandComplete("COMMIT")
	log.Printf("pgwire: conn %d COMMIT transaction (%d ops)", c.id, numOps)
}

// handleRollback aborts an explicit client transaction, discarding all
// buffered operations.
func (c *Conn) handleRollback(ctx context.Context) {
	if !c.inTxn {
		c.writeError("WARNING", "25P01", "there is no transaction in progress")
		return
	}

	// If there is an active 2PC transaction, abort it.
	if c.txnID != "" && c.txnCoordinator != nil {
		if err := c.txnCoordinator.Abort(ctx, c.txnID); err != nil {
			log.Printf("pgwire: conn %d rollback 2PC error: %v", c.id, err)
		}
	}

	numOps := len(c.bufferedOps)
	c.resetTxnState()
	log.Printf("pgwire: conn %d ROLLBACK transaction (discarded %d ops)", c.id, numOps)
}

// resetTxnState clears all transaction-related state.
func (c *Conn) resetTxnState() {
	c.inTxn = false
	c.txnFailed = false
	c.txnID = ""
	c.bufferedOps = nil
}

// txnStatusByte returns the ReadyForQuery status byte based on current
// transaction state: 'I' = idle, 'T' = in transaction, 'E' = failed transaction.
func (c *Conn) txnStatusByte() byte {
	if !c.inTxn {
		return 'I'
	}
	if c.txnFailed {
		return 'E'
	}
	return 'T'
}

// isWriteStatement returns true if the statement is a DML write operation.
func isWriteStatement(stmt *parser.Statement) bool {
	switch stmt.Type {
	case parser.StmtInsert, parser.StmtUpdate, parser.StmtDelete:
		return true
	default:
		return false
	}
}

// bufferWrite adds a write operation to the transaction buffer for later
// 2PC commit. Sends an immediate CommandComplete tag to the client.
func (c *Conn) bufferWrite(plan *planner.Plan, stmt *parser.Statement) {
	table := ""
	if len(stmt.Tables) > 0 {
		table = stmt.Tables[0]
	}

	sqlStr := planner.BuildShardSQL(plan)

	// Determine target shards.
	shards := plan.Shards
	if len(shards) == 0 {
		// All shards.
		shardCount := 1 // Minimal fallback.
		if c.catalog != nil {
			// Use a reasonable default; the actual shard count is not directly
			// accessible here, so we buffer per-shard ops for each shard
			// in the plan. For unresolved shards (no WHERE predicate), buffer
			// as a single operation to let the coordinator handle it.
			shards = []int{0}
		}
		_ = shardCount
	}

	for _, shardID := range shards {
		c.bufferedOps = append(c.bufferedOps, BufferedOp{
			Table:   table,
			ShardID: shardID,
			SQL:     sqlStr,
		})
	}

	// Send a tag back to the client as if the operation succeeded.
	// The actual execution happens on COMMIT.
	tag := "OK"
	switch stmt.Type {
	case parser.StmtInsert:
		tag = "INSERT 0 1"
	case parser.StmtUpdate:
		tag = "UPDATE 1"
	case parser.StmtDelete:
		tag = "DELETE 1"
	}
	c.writeCommandComplete(tag)
}

// --------------------------------------------------------------------------
// PL/pgSQL statement handlers
// --------------------------------------------------------------------------

// sendNotices sends NoticeResponse messages to the client for each NOTICE,
// WARNING, INFO, LOG, or DEBUG message collected during PL/pgSQL execution.
func (c *Conn) sendNotices(notices []plpgsql.NoticeMessage) {
	for _, notice := range notices {
		c.writeNotice(notice.Level, notice.Message)
	}
}

// handleCreateFunction handles CREATE FUNCTION/PROCEDURE statements by
// parsing and registering them in the function catalog.
func (c *Conn) handleCreateFunction(ctx context.Context, stmt *parser.Statement) {
	sql := stmt.CreateSQL
	if sql == "" {
		sql = stmt.RawSQL
	}
	result, err := c.executor.ExecuteCreateFunction(ctx, sql)
	if err != nil {
		c.writeError("ERROR", "42P13", fmt.Sprintf("create function error: %v", err))
		if c.inTxn {
			c.txnFailed = true
		}
		return
	}
	c.writeCommandComplete(result.Tag)
}

// handleDO handles DO $$ ... $$ anonymous block execution.
func (c *Conn) handleDO(ctx context.Context, stmt *parser.Statement) {
	body := stmt.FuncBody
	if body == "" {
		c.writeError("ERROR", "42601", "DO block has empty body")
		if c.inTxn {
			c.txnFailed = true
		}
		return
	}

	notices, err := c.executor.ExecuteDO(ctx, body)
	c.sendNotices(notices)
	if err != nil {
		c.writeError("ERROR", "P0001", fmt.Sprintf("DO block error: %v", err))
		if c.inTxn {
			c.txnFailed = true
		}
		return
	}
	c.writeCommandComplete("DO")
}

// handleCall handles CALL procedure_name(args) statements.
func (c *Conn) handleCall(ctx context.Context, stmt *parser.Statement) {
	funcName := stmt.FuncName
	argExprs := stmt.FuncArgs

	result, notices, err := c.executor.ExecuteCall(ctx, funcName, argExprs)
	c.sendNotices(notices)
	if err != nil {
		c.writeError("ERROR", "42883", fmt.Sprintf("call error: %v", err))
		if c.inTxn {
			c.txnFailed = true
		}
		return
	}

	// Send results if the function returns a result set.
	if result.Columns != nil && len(result.Columns) > 0 {
		c.writeRowDescription(result.Columns)
		numCols := len(result.Columns)
		for i := 0; i+numCols <= len(result.Rows); i += numCols {
			c.writeDataRow(result.Rows[i : i+numCols])
		}
	}
	c.writeCommandComplete(result.Tag)
}

func (c *Conn) handleParse(body []byte) {
	// Extended query protocol: Parse message.
	// Format: name (null-terminated), query (null-terminated), int16 param count, param OIDs.
	// Extract and store the prepared statement query for later Execute.
	nameEnd := indexOf(body, 0)
	if nameEnd >= 0 && nameEnd+1 < len(body) {
		remaining := body[nameEnd+1:]
		queryEnd := indexOf(remaining, 0)
		if queryEnd >= 0 {
			query := string(remaining[:queryEnd])
			stmtName := string(body[:nameEnd])
			if stmtName == "" {
				stmtName = "_unnamed"
			}
			if c.preparedStmts == nil {
				c.preparedStmts = make(map[string]string)
			}
			c.preparedStmts[stmtName] = query
		}
	}
	c.writeParseComplete()
}

func (c *Conn) handleBind(body []byte) {
	// Extended query protocol: Bind message.
	// Format: portal (null-terminated), source statement (null-terminated), ...
	// Extract portal and statement name for later Execute.
	portalEnd := indexOf(body, 0)
	if portalEnd >= 0 && portalEnd+1 < len(body) {
		remaining := body[portalEnd+1:]
		stmtEnd := indexOf(remaining, 0)
		if stmtEnd >= 0 {
			portalName := string(body[:portalEnd])
			stmtName := string(remaining[:stmtEnd])
			if portalName == "" {
				portalName = "_unnamed"
			}
			if stmtName == "" {
				stmtName = "_unnamed"
			}
			if c.portalBindings == nil {
				c.portalBindings = make(map[string]string)
			}
			c.portalBindings[portalName] = stmtName
		}
	}
	c.writeBindComplete()
}

func (c *Conn) handleDescribe(body []byte) {
	// Extended query protocol: Describe message.
	// We don't have full metadata yet, so send NoData.
	c.writeNoData()
}

func (c *Conn) handleExecute(ctx context.Context, body []byte) error {
	// Extended query protocol: Execute message.
	// Format: portal (null-terminated), max rows (int32).
	portalName := ""
	portalEnd := indexOf(body, 0)
	if portalEnd >= 0 {
		portalName = string(body[:portalEnd])
	}
	if portalName == "" {
		portalName = "_unnamed"
	}

	// Resolve portal -> statement -> query.
	stmtName := ""
	if c.portalBindings != nil {
		stmtName = c.portalBindings[portalName]
	}
	if stmtName == "" {
		stmtName = "_unnamed"
	}

	query := ""
	if c.preparedStmts != nil {
		query = c.preparedStmts[stmtName]
	}
	if query == "" {
		c.writeCommandComplete("SELECT 0")
		return nil
	}

	// Parse and execute through the normal pipeline.
	stmts, err := c.parser.Parse(query)
	if err != nil {
		c.writeError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
		return nil
	}

	for _, stmt := range stmts {
		plan, err := c.planner.Plan(stmt)
		if err != nil {
			c.writeError("ERROR", "42000", fmt.Sprintf("planning error: %v", err))
			continue
		}
		execCtx := ctx
		if c.server.stmtTimeout > 0 {
			var stmtCancel context.CancelFunc
			execCtx, stmtCancel = context.WithTimeout(ctx, c.server.stmtTimeout)
			defer stmtCancel()
		}
		result, err := c.executor.Execute(execCtx, plan)
		if err != nil {
			c.writeError("ERROR", "XX000", fmt.Sprintf("execution error: %v", err))
			continue
		}
		if result.Columns != nil && len(result.Columns) > 0 {
			c.writeRowDescription(result.Columns)
			numCols := len(result.Columns)
			for i := 0; i+numCols <= len(result.Rows); i += numCols {
				c.writeDataRow(result.Rows[i : i+numCols])
			}
		}
		c.writeCommandComplete(result.Tag)
	}
	return nil
}

// indexOf returns the index of the first occurrence of b in data, or -1.
func indexOf(data []byte, b byte) int {
	for i, v := range data {
		if v == b {
			return i
		}
	}
	return -1
}
