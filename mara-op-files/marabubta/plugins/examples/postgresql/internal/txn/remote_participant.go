// Marabunta - Licensed under the MIT License.
package txn

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// ScatterQuery mirrors executor.ScatterQuery to avoid circular imports.
type ScatterQuery struct {
	SQL      string `json:"sql"`
	Table    string `json:"table"`
	ShardID  int    `json:"shard_id"`
	Instance string `json:"instance"`
	TxnOp    string `json:"txn_op,omitempty"`
	TxnID    string `json:"txn_id,omitempty"`
}

// ScatterQueryResult mirrors executor.ScatterQueryResult.
type ScatterQueryResult struct {
	Columns  []string   `json:"columns"`
	Rows     [][]string `json:"rows"`
	Error    string     `json:"error,omitempty"`
	Affected int64      `json:"affected"`
}

// RemoteParticipant sends 2PC operations to shards on remote nodes via the
// swarm's scatter API. It implements the ParticipantAdapter interface.
type RemoteParticipant struct {
	client   *swarm.SwarmClient
	instance string
}

// NewRemoteParticipant creates a remote participant.
func NewRemoteParticipant(client *swarm.SwarmClient, instance string) *RemoteParticipant {
	return &RemoteParticipant{client: client, instance: instance}
}

// Prepare sends a prepare request to a remote node hosting the shard.
func (rp *RemoteParticipant) Prepare(ctx context.Context, txnID, table string, shardID int, sql string, args []interface{}) error {
	return rp.send2PCOp(ctx, "prepare", txnID, table, shardID, sql)
}

// Commit sends a commit to a remote node.
func (rp *RemoteParticipant) Commit(ctx context.Context, txnID string) error {
	return rp.send2PCOp(ctx, "commit", txnID, "", 0, "")
}

// Abort sends an abort to a remote node.
func (rp *RemoteParticipant) Abort(ctx context.Context, txnID string) error {
	return rp.send2PCOp(ctx, "abort", txnID, "", 0, "")
}

// send2PCOp sends a 2PC operation via scatter.
func (rp *RemoteParticipant) send2PCOp(ctx context.Context, op, txnID, table string, shardID int, sql string) error {
	sq := ScatterQuery{
		SQL:      sql,
		Table:    table,
		ShardID:  shardID,
		Instance: rp.instance,
		TxnOp:    op,
		TxnID:    txnID,
	}
	payload, err := json.Marshal(sq)
	if err != nil {
		return fmt.Errorf("marshal 2PC %s: %w", op, err)
	}

	timeoutMS := uint32(30000)
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 {
			timeoutMS = uint32(remaining.Milliseconds())
		}
	}

	resp, err := rp.client.Scatter(swarm.ScatterRequest{
		Units: []swarm.ScatterUnit{{
			Payload:        payload,
			RequiredTraits: []string{"CanStoreState"},
		}},
		Hints: swarm.ScatterHints{
			TimeoutMS: timeoutMS,
		},
	})
	if err != nil {
		return fmt.Errorf("scatter 2PC %s: %w", op, err)
	}

	// Check response.
	if len(resp.Results) == 0 {
		return fmt.Errorf("2PC %s: no response", op)
	}
	r := resp.Results[0]
	if !r.Success {
		return fmt.Errorf("2PC %s failed: %s", op, r.Error)
	}

	// Check inner result for errors.
	var result ScatterQueryResult
	if err := json.Unmarshal(r.Response, &result); err != nil {
		return fmt.Errorf("2PC %s unmarshal: %w", op, err)
	}
	if result.Error != "" {
		return fmt.Errorf("2PC %s: %s", op, result.Error)
	}
	return nil
}
