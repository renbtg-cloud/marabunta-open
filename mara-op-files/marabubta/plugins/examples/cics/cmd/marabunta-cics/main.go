// Marabunta - Licensed under the MIT License.
// Command marabunta-cics is the entry point for the Marabunta CICS-compatible
// transaction processing plugin. It registers with the swarm, initializes
// VSAM storage, queue managers, WASM runtime, and starts a TN3270 listener.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	swarm "github.com/marabunta/swarm-plugin-go"

	"github.com/marabunta/marabunta-cics/internal/plugin"
	"github.com/marabunta/marabunta-cics/internal/queue"
	"github.com/marabunta/marabunta-cics/internal/terminal"
	"github.com/marabunta/marabunta-cics/internal/transaction"
	"github.com/marabunta/marabunta-cics/internal/vsam"
	"github.com/marabunta/marabunta-cics/internal/wasm"
)

func main() {
	swarmAddr := flag.String("swarm-addr", "/tmp/marabunta.sock", "Swarm host address (unix socket or host:port)")
	listenAddr := flag.String("listen", ":3270", "TN3270 listen address")
	wasmDir := flag.String("wasm-dir", "./programs", "Directory for WASM program modules")
	regionID := flag.String("region", "CICSHVR1", "CICS region ID")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("marabunta-cics starting region=%s listen=%s", *regionID, *listenAddr)

	// Connect to swarm.
	client, err := plugin.NewClient(*swarmAddr)
	if err != nil {
		log.Fatalf("failed to connect to swarm: %v", err)
	}
	defer client.Close()

	// Register plugin.
	regResp, err := client.Register(swarm.RegisterRequest{
		Name:    "cics",
		Version: "1.0.0",
		Traits:  []string{"CanStoreState", "CanExecute"},
		Endpoints: []swarm.Endpoint{
			{
				Name:        "tn3270",
				Protocol:    "tcp",
				DefaultPort: 3270,
			},
		},
	})
	if err != nil {
		log.Fatalf("failed to register with swarm: %v", err)
	}
	log.Printf("registered: plugin_id=%s node_id=%s", regResp.PluginID, regResp.NodeID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize VSAM manager.
	vsamMgr := vsam.NewManager(client.SwarmClient())

	// Initialize queue managers.
	tsMgr := queue.NewTSManager(client.SwarmClient())
	tdMgr := queue.NewTDManager(client.SwarmClient())

	// Initialize WASM runtime.
	wasmRuntime, err := wasm.NewRuntime(ctx, *wasmDir, client.SwarmClient(), vsamMgr, tsMgr, tdMgr)
	if err != nil {
		log.Fatalf("failed to initialize WASM runtime: %v", err)
	}
	defer wasmRuntime.Close()

	// Initialize transaction executor.
	txExecutor := transaction.NewExecutor(client.SwarmClient(), vsamMgr, tsMgr, tdMgr, wasmRuntime, *regionID)

	// Start TN3270 server.
	srv := terminal.NewServer(*listenAddr, txExecutor, *regionID)
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("TN3270 server error: %v", err)
		}
	}()
	log.Printf("TN3270 server listening on %s", *listenAddr)

	// Start event loop for swarm-initiated requests.
	client.StartEventLoop(func(msg *swarm.WireMessage) *swarm.WireMessage {
		switch msg.Type {
		case swarm.TypeHealthReq:
			resp := &swarm.HealthResponse{
				Healthy: true,
				Status:  "running",
				Details: map[string]string{
					"region":      *regionID,
					"listen_addr": *listenAddr,
					"wasm_dir":    *wasmDir,
					"programs":    fmt.Sprintf("%d", wasmRuntime.CacheSize()),
				},
			}
			return &swarm.WireMessage{Type: swarm.TypeHealthResp, Payload: resp}
		case swarm.TypeStopReq:
			log.Println("received stop request from swarm")
			cancel()
			srv.Close()
			return &swarm.WireMessage{
				Type:    swarm.TypeStopResp,
				Payload: swarm.StopResponse{Clean: true},
			}
		default:
			return nil
		}
	})

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received signal %v, shutting down", sig)
	cancel()
	srv.Close()
	log.Println("marabunta-cics stopped")
}
