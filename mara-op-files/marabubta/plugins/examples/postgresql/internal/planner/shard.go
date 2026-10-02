// Marabunta - Licensed under the MIT License.
// Package planner — shard pruning logic.
// Implements P.12.1: WHERE shard_key = C routes to exactly 1 shard;
// WHERE shard_key IN (C1, C2, ...) routes to N shards.
package planner

import (
	"hash/fnv"

	"github.com/marabunta/marabunta-postgres/internal/parser"
)

// pruneShards analyzes the WHERE clause to determine which shards need
// to be accessed. Returns nil if all shards must be scanned.
func (p *Planner) pruneShards(table string, where *parser.WhereClause) []int {
	if where == nil {
		return nil
	}

	shardKey := p.catalog.GetShardKey(table)
	if shardKey == "" {
		return nil
	}

	return p.extractShards(where, shardKey)
}

// extractShards recursively walks the WHERE clause tree to find shard key
// constraints. AND narrows (intersection), OR widens (union).
func (p *Planner) extractShards(where *parser.WhereClause, shardKey string) []int {
	if where == nil {
		return nil
	}

	switch where.Op {
	case "=":
		// P.12.1: shard_key = constant → exactly 1 shard.
		if where.Column.Column == shardKey {
			shardID := p.hashToShard(where.Value)
			return []int{shardID}
		}
		return nil

	case "IN":
		// P.12.1: shard_key IN (v1, v2, ...) → N shards.
		if where.Column.Column == shardKey {
			shardSet := make(map[int]bool)
			for _, v := range where.Values {
				shardSet[p.hashToShard(v)] = true
			}
			var shards []int
			for s := range shardSet {
				shards = append(shards, s)
			}
			return shards
		}
		return nil

	case "AND":
		// Intersection: both sides must agree.
		var result []int
		for _, child := range where.Children {
			shards := p.extractShards(child, shardKey)
			if shards != nil {
				if result == nil {
					result = shards
				} else {
					result = intersectShards(result, shards)
				}
			}
		}
		return result

	case "OR":
		// Union: either side qualifies.
		var result []int
		allHaveShards := true
		for _, child := range where.Children {
			shards := p.extractShards(child, shardKey)
			if shards == nil {
				allHaveShards = false
				break
			}
			result = unionShards(result, shards)
		}
		if !allHaveShards {
			return nil // One branch has no shard constraint → scan all.
		}
		return result

	default:
		return nil
	}
}

// hashToShard maps a string value to a shard ID using FNV-1a.
func (p *Planner) hashToShard(value string) int {
	h := fnv.New32a()
	h.Write([]byte(value))
	return int(h.Sum32()) % p.shardCount
}

// intersectShards returns the intersection of two shard lists.
func intersectShards(a, b []int) []int {
	set := make(map[int]bool)
	for _, s := range a {
		set[s] = true
	}
	var result []int
	for _, s := range b {
		if set[s] {
			result = append(result, s)
		}
	}
	return result
}

// unionShards returns the union of two shard lists.
func unionShards(a, b []int) []int {
	set := make(map[int]bool)
	for _, s := range a {
		set[s] = true
	}
	for _, s := range b {
		set[s] = true
	}
	var result []int
	for s := range set {
		result = append(result, s)
	}
	return result
}
