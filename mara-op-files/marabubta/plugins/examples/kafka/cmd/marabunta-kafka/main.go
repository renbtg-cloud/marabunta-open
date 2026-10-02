// Marabunta - Licensed under the MIT License.
// Command marabunta-kafka is the entry point for the Marabunta Kafka-compatible
// broker plugin. It registers with the swarm, initializes the broker, and
// starts a Kafka binary protocol listener.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	swarm "github.com/marabunta/swarm-plugin-go"

	"github.com/marabunta/marabunta-kafka/internal/broker"
	"github.com/marabunta/marabunta-kafka/internal/consumer"
	"github.com/marabunta/marabunta-kafka/internal/plugin"
	"github.com/marabunta/marabunta-kafka/internal/retention"
	"github.com/marabunta/marabunta-kafka/internal/storage"
	"github.com/marabunta/marabunta-kafka/internal/wire"
)

func main() {
	swarmAddr := flag.String("swarm-addr", "/tmp/marabunta.sock", "Swarm host address (unix socket or host:port)")
	listenAddr := flag.String("listen", ":9092", "Kafka protocol listen address")
	clusterID := flag.String("cluster-id", "marabunta-kafka-1", "Kafka cluster ID")
	nodeIDOverride := flag.String("node-id", "", "Override node ID (default: assigned by swarm)")
	retentionHours := flag.Int("retention-hours", 168, "Default log retention in hours (7 days)")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("marabunta-kafka starting cluster=%s listen=%s", *clusterID, *listenAddr)

	// Connect to swarm.
	client, err := plugin.NewClient(*swarmAddr)
	if err != nil {
		log.Fatalf("failed to connect to swarm: %v", err)
	}
	defer client.Close()

	// Register plugin.
	regResp, err := client.Register(swarm.RegisterRequest{
		Name:    "kafka",
		Version: "1.0.0",
		Traits:  []string{"CanStoreState", "CanExecute"},
		Endpoints: []swarm.Endpoint{
			{
				Name:        "kafka",
				Protocol:    "tcp",
				DefaultPort: 9092,
			},
		},
	})
	if err != nil {
		log.Fatalf("failed to register with swarm: %v", err)
	}
	nodeID := regResp.NodeID
	if *nodeIDOverride != "" {
		nodeID = *nodeIDOverride
	}
	log.Printf("registered: plugin_id=%s node_id=%s", regResp.PluginID, nodeID)

	// Initialize storage layer.
	logStore := storage.NewLogStore(client.SwarmClient())

	// Initialize broker.
	brk := broker.NewBroker(*clusterID, nodeID, client.SwarmClient(), logStore)

	// Initialize consumer group coordinator.
	consumerCoord := consumer.NewCoordinator(client.SwarmClient(), nodeID)

	// Initialize offset manager.
	offsetMgr := storage.NewOffsetManager(client.SwarmClient(), 5*time.Second)
	defer offsetMgr.Stop()

	// Initialize retention engine and wire it up to the broker for topic discovery.
	retentionEng := retention.NewEngine(logStore, *retentionHours)
	retentionEng.SetTopicSource(brk)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start retention enforcement.
	go retentionEng.Run(ctx)

	// Start consumer group coordination.
	go consumerCoord.Run(ctx)

	// Start Kafka protocol server.
	srv := wire.NewServer(*listenAddr, brk, logStore, consumerCoord, offsetMgr)
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("kafka server error: %v", err)
		}
	}()
	log.Printf("kafka protocol server listening on %s", *listenAddr)

	// Start event loop for swarm-initiated requests.
	client.StartEventLoop(func(msg *swarm.WireMessage) *swarm.WireMessage {
		switch msg.Type {
		case swarm.TypeHealthReq:
			resp := &swarm.HealthResponse{
				Healthy: true,
				Status:  "running",
				Details: map[string]string{
					"cluster_id":     *clusterID,
					"node_id":        nodeID,
					"listen_addr":    *listenAddr,
					"topic_count":    fmt.Sprintf("%d", brk.TopicCount()),
					"retention_hours": fmt.Sprintf("%d", *retentionHours),
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
	log.Println("marabunta-kafka stopped")
}
