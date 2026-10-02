// Marabunta - Licensed under the MIT License.
// Command marabunta-postgres is the entry point for the Marabunta distributed
// PostgreSQL plugin. It registers with the swarm, initializes the shard
// manager and catalog, and starts a pgwire-compatible listener.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	swarm "github.com/marabunta/swarm-plugin-go"

	"github.com/marabunta/marabunta-postgres/internal/catalog"
	"github.com/marabunta/marabunta-postgres/internal/executor"
	"github.com/marabunta/marabunta-postgres/internal/parser"
	"github.com/marabunta/marabunta-postgres/internal/planner"
	"github.com/marabunta/marabunta-postgres/internal/plugin"
	"github.com/marabunta/marabunta-postgres/internal/replication"
	"github.com/marabunta/marabunta-postgres/internal/storage"
	"github.com/marabunta/marabunta-postgres/internal/txn"
	"github.com/marabunta/marabunta-postgres/internal/wire"
)

func main() {
	swarmAddr := flag.String("swarm-addr", "/tmp/marabunta.sock", "Swarm host address (unix socket or host:port)")
	listenAddr := flag.String("listen", ":5432", "pgwire listen address")
	instance := flag.String("instance", "default", "PostgreSQL instance name")
	dataDir := flag.String("data-dir", "./pgdata", "Directory for SQLite shard files")
	shardCount := flag.Int("shards", 16, "Number of shards per table")
	replicas := flag.Int("replicas", 2, "Number of replicas per shard")
	readFromReplica := flag.Bool("read-from-replica", false, "Route SELECT queries to replica shards")
	replicationLagMax := flag.Int("replication-lag-max", 1000, "Maximum acceptable replication lag in LSN entries")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("marabunta-postgres starting instance=%s listen=%s shards=%d replicas=%d",
		*instance, *listenAddr, *shardCount, *replicas)

	// Connect to swarm.
	client, err := plugin.NewClient(*swarmAddr)
	if err != nil {
		log.Fatalf("failed to connect to swarm: %v", err)
	}
	defer client.Close()

	// Register plugin.
	regResp, err := client.Register(swarm.RegisterRequest{
		Name:    "postgres",
		Version: "1.0.0",
		Traits:  []string{"CanStoreState", "CanExecute"},
		Endpoints: []swarm.Endpoint{
			{
				Name:        "pgwire",
				Protocol:    "tcp",
				DefaultPort: 5432,
			},
		},
	})
	if err != nil {
		log.Fatalf("failed to register with swarm: %v", err)
	}
	log.Printf("registered: plugin_id=%s node_id=%s", regResp.PluginID, regResp.NodeID)

	// Initialize storage layer.
	shardMgr, err := storage.NewShardManager(*dataDir, *shardCount)
	if err != nil {
		log.Fatalf("failed to initialize shard manager: %v", err)
	}
	defer shardMgr.Close()

	// Initialize catalog.
	cat := catalog.NewCatalog(*instance, client.SwarmClient())
	if err := cat.Load(); err != nil {
		log.Printf("warning: catalog load failed (starting fresh): %v", err)
	}

	// Initialize subsystems.
	prs := parser.New()
	exec := executor.New(client.SwarmClient(), shardMgr, cat, *instance)
	plan := planner.New(cat, *shardCount)

	// Initialize 2PC distributed transaction subsystem.
	txnLogPath := filepath.Join(*dataDir, "txn_log.db")
	txnLog, err := txn.NewTxnLog(txnLogPath)
	if err != nil {
		log.Fatalf("failed to initialize transaction log: %v", err)
	}
	defer txnLog.Close()

	txnParticipant := txn.NewTxnParticipant(shardMgr, 0) // 0 = default 60s stale timeout
	defer txnParticipant.Stop()

	txnCoordinator := txn.NewTxnCoordinator(txnParticipant, txnLog)
	exec.SetTxnCoordinator(txnCoordinator)

	// Wire cross-node 2PC: remote participant sends prepare/commit/abort via scatter.
	remoteParticipant := txn.NewRemoteParticipant(client.SwarmClient(), *instance)
	txnCoordinator.SetRemoteParticipant(remoteParticipant)

	// Run crash recovery for any in-flight transactions from a previous run.
	txnRecovery := txn.NewTxnRecovery(txnLog, txnParticipant)
	recoveryCtx, recoveryCancel := context.WithTimeout(context.Background(), 30*time.Second)
	recoveryResult, recoveryErr := txnRecovery.Recover(recoveryCtx)
	recoveryCancel()
	if recoveryErr != nil {
		log.Printf("warning: transaction recovery encountered errors: %v", recoveryErr)
	}
	if recoveryResult != nil && (recoveryResult.Committed > 0 || recoveryResult.Aborted > 0) {
		log.Printf("transaction recovery: committed=%d, aborted=%d, errors=%d",
			recoveryResult.Committed, recoveryResult.Aborted, len(recoveryResult.Errors))
	}

	// Initialize replication subsystem.
	placementDir := filepath.Join(*dataDir, "replication")
	placementMgr, err := replication.NewPlacementManager(placementDir, *replicas)
	if err != nil {
		log.Fatalf("failed to initialize placement manager: %v", err)
	}
	defer placementMgr.Close()

	walShipper := replication.NewWALShipper(client.SwarmClient(), replication.WALShipperConfig{
		FlushInterval: replication.DefaultWALShipperConfig().FlushInterval,
		BatchSize:     replication.DefaultWALShipperConfig().BatchSize,
		Instance:      *instance,
		NodeID:        regResp.NodeID,
		OutboxPath:    filepath.Join(*dataDir, "wal_outbox.db"),
	})
	defer walShipper.Stop()

	// Wire the write hook: after each successful write on a primary shard,
	// ship the SQL statement to replicas via the WAL shipper.
	shardMgr.SetWriteHook(func(table string, shardID int, sql string) {
		entry := replication.WALEntry{
			Table:   table,
			ShardID: shardID,
			SQL:     sql,
		}
		if err := walShipper.Ship(entry); err != nil {
			log.Printf("wal_shipper: failed to ship entry for %s:%d: %v", table, shardID, err)
		}
	})

	follower := replication.NewFollower(client.SwarmClient(), shardMgr, replication.FollowerConfig{
		MaxLag:   uint64(*replicationLagMax),
		Instance: *instance,
	})
	defer follower.Stop()

	failoverConfig := replication.DefaultFailoverConfig()
	failoverConfig.Instance = *instance
	failoverMgr := replication.NewFailoverManager(
		client.SwarmClient(),
		placementMgr,
		follower,
		failoverConfig,
		regResp.NodeID,
	)
	defer failoverMgr.Stop()

	// Create top-level context for background subsystems.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize placement sync: broadcasts local placement changes and
	// subscribes to remote changes via Pub/Sub + KV store.
	placementSync := replication.NewPlacementSync(placementMgr, client.SwarmClient(), *instance, regResp.NodeID)
	if err := placementSync.Start(ctx); err != nil {
		log.Printf("warning: placement sync failed to start: %v", err)
	}
	defer placementSync.Stop()

	// Wire ScatterExecutor into Executor for distributed query execution.
	scatterExec := executor.NewScatterExecutor(client.SwarmClient(), *instance)
	exec.SetScatterExecutor(scatterExec)
	exec.SetPlacementManager(placementMgr)

	// Configure read-from-replica if enabled.
	if *readFromReplica {
		exec.SetReadFromReplica(true)
		log.Println("read-from-replica enabled: SELECT queries will be routed to replicas when available")
	}

	log.Printf("replication initialized: replicas=%d, max_lag=%d, read_from_replica=%v",
		*replicas, *replicationLagMax, *readFromReplica)

	// Start catalog sync in the background.
	go cat.StartSync(ctx)

	// Start pgwire server.
	srv := wire.NewServer(*listenAddr, prs, plan, exec, cat)
	srv.SetTxnCoordinator(txnCoordinator)
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("pgwire server error: %v", err)
		}
	}()
	log.Printf("pgwire server listening on %s", *listenAddr)

	// Start event loop for swarm-initiated requests.
	client.StartEventLoop(func(msg *swarm.WireMessage) *swarm.WireMessage {
		switch msg.Type {
		case swarm.TypeHealthReq:
			resp := &swarm.HealthResponse{
				Healthy: true,
				Status:  "running",
				Details: map[string]string{
					"instance":          *instance,
					"shards":            fmt.Sprintf("%d", *shardCount),
					"listen_addr":       *listenAddr,
					"replicas":          fmt.Sprintf("%d", *replicas),
					"read_from_replica": fmt.Sprintf("%v", *readFromReplica),
				},
			}
			return &swarm.WireMessage{Type: swarm.TypeHealthResp, Payload: resp}

		case swarm.TypeStopReq:
			log.Println("received stop request from swarm")
			placementSync.Stop()
			walShipper.Stop()
			follower.Stop()
			failoverMgr.Stop()
			cancel()
			srv.Close()
			return &swarm.WireMessage{
				Type:    swarm.TypeStopResp,
				Payload: swarm.StopResponse{Clean: true},
			}

		case swarm.TypeHandleReq:
			return handleScatterRequest(msg, *instance, shardMgr, txnParticipant, walShipper, follower)

		default:
			return nil
		}
	})

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received signal %v, shutting down", sig)
	placementSync.Stop()
	walShipper.Stop()
	follower.Stop()
	failoverMgr.Stop()
	cancel()
	srv.Close()
	log.Println("marabunta-postgres stopped")
}

// handleScatterRequest processes HandleReq messages from the swarm. These are
// scatter queries sent by other nodes' ScatterExecutor.FanOut() calls, or 2PC
// operations forwarded to participant shards on this node.
func handleScatterRequest(
	msg *swarm.WireMessage,
	instance string,
	shardMgr *storage.ShardManager,
	txnParticipant *txn.TxnParticipant,
	walShipper *replication.WALShipper,
	follower *replication.Follower,
) *swarm.WireMessage {
	// Decode the HandleRequest envelope.
	var handleReq swarm.HandleRequest
	if err := swarm.DecodePayload(msg, &handleReq); err != nil {
		return handleErrorResponse(fmt.Sprintf("decode HandleReq: %v", err))
	}

	// Unmarshal the inner ScatterQuery.
	var sq executor.ScatterQuery
	if err := json.Unmarshal(handleReq.Payload, &sq); err != nil {
		return handleErrorResponse(fmt.Sprintf("unmarshal ScatterQuery: %v", err))
	}

	// Validate instance match.
	if sq.Instance != instance {
		return handleErrorResponse(fmt.Sprintf("instance mismatch: got %q, want %q", sq.Instance, instance))
	}

	// Route 2PC and special operations.
	if sq.TxnOp != "" {
		return handle2PCOp(sq, txnParticipant, walShipper, follower)
	}

	// Normal scatter query execution on the local shard.
	ctx := context.Background()
	cols, rows, err := shardMgr.Query(ctx, sq.Table, sq.ShardID, sq.SQL)

	result := buildScatterResult(cols, rows, err)
	respPayload, _ := json.Marshal(result)
	return &swarm.WireMessage{
		Type: swarm.TypeHandleResp,
		Payload: swarm.HandleResponse{Payload: respPayload},
	}
}

// handle2PCOp dispatches 2PC operations (prepare/commit/abort) and special
// queries (snapshot, lsn_query) received via HandleReq.
func handle2PCOp(
	sq executor.ScatterQuery,
	txnParticipant *txn.TxnParticipant,
	walShipper *replication.WALShipper,
	follower *replication.Follower,
) *swarm.WireMessage {
	ctx := context.Background()
	var err error

	switch sq.TxnOp {
	case "prepare":
		err = txnParticipant.Prepare(ctx, sq.TxnID, sq.Table, sq.ShardID, sq.SQL, nil)
	case "commit":
		err = txnParticipant.Commit(ctx, sq.TxnID)
	case "abort":
		err = txnParticipant.Abort(ctx, sq.TxnID)
	case "lsn_query":
		// Return the follower's current applied LSN for this shard.
		lsn := uint64(0)
		if follower != nil {
			lsn = follower.GetLSN(sq.Table, sq.ShardID)
		}
		result := executor.ScatterQueryResult{
			Columns: []string{"lsn"},
			Rows:    [][]string{{fmt.Sprintf("%d", lsn)}},
		}
		respPayload, _ := json.Marshal(result)
		return &swarm.WireMessage{
			Type:    swarm.TypeHandleResp,
			Payload: swarm.HandleResponse{Payload: respPayload},
		}
	case "snapshot":
		// Snapshot request for data bootstrapping — return shard data.
		// For now, return an empty success (full snapshot impl in M1).
		result := executor.ScatterQueryResult{
			Columns: []string{"status"},
			Rows:    [][]string{{"snapshot_not_implemented"}},
		}
		respPayload, _ := json.Marshal(result)
		return &swarm.WireMessage{
			Type:    swarm.TypeHandleResp,
			Payload: swarm.HandleResponse{Payload: respPayload},
		}
	default:
		return handleErrorResponse(fmt.Sprintf("unknown txn_op: %q", sq.TxnOp))
	}

	// Build response for prepare/commit/abort.
	result := executor.ScatterQueryResult{}
	if err != nil {
		result.Error = err.Error()
	}
	respPayload, _ := json.Marshal(result)
	return &swarm.WireMessage{
		Type:    swarm.TypeHandleResp,
		Payload: swarm.HandleResponse{Payload: respPayload},
	}
}

// buildScatterResult converts raw shard query output to a ScatterQueryResult.
func buildScatterResult(cols []string, rows [][]byte, err error) executor.ScatterQueryResult {
	if err != nil {
		return executor.ScatterQueryResult{Error: err.Error()}
	}

	// Convert flat [][]byte to [][]string rows.
	var strRows [][]string
	numCols := len(cols)
	if numCols > 0 {
		for r := 0; r+numCols <= len(rows); r += numCols {
			strRow := make([]string, numCols)
			for c := 0; c < numCols; c++ {
				if rows[r+c] != nil {
					strRow[c] = string(rows[r+c])
				}
			}
			strRows = append(strRows, strRow)
		}
	}

	return executor.ScatterQueryResult{
		Columns: cols,
		Rows:    strRows,
	}
}

// handleErrorResponse creates a HandleResp WireMessage containing an error.
func handleErrorResponse(errMsg string) *swarm.WireMessage {
	result := executor.ScatterQueryResult{Error: errMsg}
	respPayload, _ := json.Marshal(result)
	return &swarm.WireMessage{
		Type:    swarm.TypeHandleResp,
		Payload: swarm.HandleResponse{Payload: respPayload},
	}
}
