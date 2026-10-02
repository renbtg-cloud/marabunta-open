// Marabunta - Licensed under the MIT License.
package transaction

import (
	"fmt"
	"log"
	"sync"
)

// BatchProcessor groups EXEC CICS operations and analyzes dependencies
// to determine which can be executed in parallel (spec C.3).
type BatchProcessor struct {
	mu   sync.Mutex
	ops  []BatchOp
}

// BatchOp represents a single operation in a batch.
type BatchOp struct {
	ID        int
	Command   string
	Dataset   string
	Key       string
	Data      []byte
	DependsOn []int // IDs of operations this depends on
	Result    string
	Error     error
	Done      bool
}

// NewBatchProcessor creates a new BatchProcessor.
func NewBatchProcessor() *BatchProcessor {
	return &BatchProcessor{}
}

// Add adds an operation to the batch.
func (bp *BatchProcessor) Add(command, dataset, key string, data []byte) int {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	id := len(bp.ops)
	op := BatchOp{
		ID:      id,
		Command: command,
		Dataset: dataset,
		Key:     key,
		Data:    data,
	}
	bp.ops = append(bp.ops, op)
	return id
}

// AddWithDeps adds an operation with explicit dependencies.
func (bp *BatchProcessor) AddWithDeps(command, dataset, key string, data []byte, deps []int) int {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	id := len(bp.ops)
	op := BatchOp{
		ID:        id,
		Command:   command,
		Dataset:   dataset,
		Key:       key,
		Data:      data,
		DependsOn: deps,
	}
	bp.ops = append(bp.ops, op)
	return id
}

// AnalyzeDependencies automatically determines dependencies between operations.
// Rules:
// - WRITE after READ to same dataset+key: depends on the READ.
// - WRITE after WRITE to same dataset+key: depends on previous WRITE.
// - REWRITE after READ to same dataset+key: depends on the READ.
// - DELETE after any op to same dataset+key: depends on previous op.
func (bp *BatchProcessor) AnalyzeDependencies() {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for i := range bp.ops {
		for j := i - 1; j >= 0; j-- {
			if bp.ops[i].Dataset == bp.ops[j].Dataset && bp.ops[i].Key == bp.ops[j].Key {
				bp.ops[i].DependsOn = append(bp.ops[i].DependsOn, j)
				break // Only need the immediately preceding operation on same key.
			}
		}
	}
}

// Execute runs the batch, respecting dependencies.
// Independent operations are executed concurrently.
func (bp *BatchProcessor) Execute(execFn func(op *BatchOp) (string, error)) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Build dependency graph.
	remaining := make(map[int]bool)
	for i := range bp.ops {
		remaining[i] = true
	}

	for len(remaining) > 0 {
		// Find operations with all dependencies satisfied.
		var ready []int
		for id := range remaining {
			allDone := true
			for _, dep := range bp.ops[id].DependsOn {
				if !bp.ops[dep].Done {
					allDone = false
					break
				}
				// Check if dependency had an error.
				if bp.ops[dep].Error != nil {
					bp.ops[id].Error = fmt.Errorf("dependency %d failed: %w", dep, bp.ops[dep].Error)
					bp.ops[id].Done = true
					delete(remaining, id)
					allDone = false
					break
				}
			}
			if allDone && !bp.ops[id].Done {
				ready = append(ready, id)
			}
		}

		if len(ready) == 0 {
			// Deadlock or all remaining have failed dependencies.
			for id := range remaining {
				bp.ops[id].Error = fmt.Errorf("unable to resolve dependencies")
				bp.ops[id].Done = true
			}
			break
		}

		// Execute ready operations (could be parallelized with goroutines).
		var wg sync.WaitGroup
		for _, id := range ready {
			wg.Add(1)
			go func(opID int) {
				defer wg.Done()
				result, err := execFn(&bp.ops[opID])
				bp.ops[opID].Result = result
				bp.ops[opID].Error = err
				bp.ops[opID].Done = true
			}(id)
		}
		wg.Wait()

		for _, id := range ready {
			delete(remaining, id)
		}
	}

	// Check for errors.
	var errors []error
	for _, op := range bp.ops {
		if op.Error != nil {
			errors = append(errors, fmt.Errorf("op %d (%s %s:%s): %w",
				op.ID, op.Command, op.Dataset, op.Key, op.Error))
		}
	}

	if len(errors) > 0 {
		log.Printf("CICS batch: %d/%d operations failed", len(errors), len(bp.ops))
		return errors[0]
	}

	return nil
}

// Results returns the results of all operations.
func (bp *BatchProcessor) Results() []BatchOp {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	result := make([]BatchOp, len(bp.ops))
	copy(result, bp.ops)
	return result
}
