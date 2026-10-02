<!-- Marabunta - Licensed under the MIT License.
# Marabunta Compute Examples

This directory contains example configurations, job specifications, workflows, and policies for Marabunta Compute.

## Directory Structure

```
examples/
├── jobs/           # Example job specifications
├── workflows/      # Multi-job workflow definitions
├── policies/       # Scheduling and placement policies
└── configs/        # Node and cluster configurations
```

## Jobs

Example job specifications in JSON format.

### hello-world.json
The simplest possible job - runs a single shell command. Start here to understand the basic job structure.

```bash
marabunta submit examples/jobs/hello-world.json
```

### map-reduce.json
Multi-task parallel job demonstrating the map-reduce pattern with word counting. Shows how to run many tasks in parallel across the cluster.

### dag-pipeline.json
Job with task dependencies forming a DAG (Directed Acyclic Graph). Demonstrates:
- Sequential task execution with `depends_on`
- Parallel branches that merge
- Data passing between tasks

### python-analysis.json
Python-based statistical analysis job. Shows:
- Python script execution
- Multi-stage data processing
- Statistical computations

### monte-carlo.json
Monte Carlo simulation with configurable parameters:
- Seed-based deterministic sampling
- Convergence detection
- Statistical aggregation

## Workflows

Multi-job workflow definitions in YAML format.

### data-pipeline.yaml
ETL workflow demonstrating:
- Multiple data sources
- Parallel extraction
- Sequential transformation and loading
- Error handling and retries
- Artifact preservation

### ml-training.yaml
Machine learning pipeline with:
- Data preparation and feature engineering
- Parallel hyperparameter tuning
- Model selection and final training
- Evaluation and deployment
- GPU resource requirements

### conditional.yaml
Workflow with conditional logic:
- Dynamic branching based on parameters
- Quality checks with thresholds
- Multiple processing modes
- Error handlers and fallbacks

## Policies

Scheduling policies in `.marabunta` format.

### gpu-affinity.marabunta
Policies for GPU workload placement:
- Tag-based node selection
- GPU preference and requirements
- Maintenance window handling

### region-failover.marabunta
Multi-region placement policies:
- Geographic routing
- GDPR/data residency compliance
- Automatic failover
- Cross-region replication

### priority-queue.marabunta
Priority-based scheduling:
- Priority tiers (critical, high, normal, low)
- Preemption rules
- Fair scheduling
- Queue aging

## Configurations

Node and cluster configurations in TOML format.

### single-node.toml
Configuration for running all components on a single machine:
- Development and testing
- Small workloads
- Learning the system

Usage:
```bash
# Start coordinator
marabunta-coordinator --config examples/configs/single-node.toml

# Start master (same machine)
marabunta-master --config examples/configs/single-node.toml

# Start worker (same machine)
marabunta-worker --config examples/configs/single-node.toml
```

### small-cluster.toml
3-node cluster configuration:
- 1 coordinator (also runs master)
- 2-3 worker nodes
- TLS enabled

### production.toml
Production-grade configuration:
- HA coordinator cluster (3+ nodes)
- Multiple masters per region
- Comprehensive security
- Metrics and monitoring
- Rate limiting and quotas

## Quick Start

1. **Single Node Development**
   ```bash
   # Copy and customize config
   cp examples/configs/single-node.toml ~/.marabunta/config.toml

   # Start all services
   marabunta-coordinator &
   marabunta-master &
   marabunta-worker &

   # Submit a job
   marabunta submit examples/jobs/hello-world.json
   ```

2. **Submit Example Jobs**
   ```bash
   # Simple job
   marabunta submit examples/jobs/hello-world.json

   # Parallel job
   marabunta submit examples/jobs/map-reduce.json

   # DAG pipeline
   marabunta submit examples/jobs/dag-pipeline.json
   ```

3. **Run a Workflow**
   ```bash
   marabunta workflow run examples/workflows/data-pipeline.yaml \
     --param date=2024-01-15 \
     --param environment=staging
   ```

4. **Apply Policies**
   ```bash
   marabunta policy apply examples/policies/gpu-affinity.marabunta
   marabunta policy apply examples/policies/priority-queue.marabunta
   ```

## Customization

These examples are starting points. Common customizations:

- **Jobs**: Modify task payloads, add constraints, adjust parallelism
- **Workflows**: Add stages, change dependencies, customize notifications
- **Policies**: Adjust weights, add conditions, change resource limits
- **Configs**: Update addresses, tune timeouts, enable/disable features

## Documentation

For more information, see the main documentation:
- [User Guide](../docs/user-guide.md)
- [API Reference](../docs/api-reference.md)
- [Operations Guide](../docs/operations.md)
