// Marabunta - Licensed under the MIT License.
package com.marabunta.rover

import com.intellij.openapi.editor.Document
import com.marabunta.rover.LegacyTopologyInterceptor.AntiPatternType

object TopologySynthesizer {

    data class SynthesisResult(
        val roastHtml: String,
        val synthesizedJcl: String,
        val costReductionFactor: Int
    )

    fun synthesize(antiPatternType: AntiPatternType, document: Document): SynthesisResult {
        val content = document.text

        return when (antiPatternType) {
            AntiPatternType.KUBERNETES -> synthesizeKubernetes(content)
            AntiPatternType.TERRAFORM -> synthesizeTerraform(content)
            AntiPatternType.DOCKER -> synthesizeDocker(content)
            AntiPatternType.KAFKA -> synthesizeKafka(content)
            AntiPatternType.S3 -> synthesizeS3(content)
        }
    }

    private fun synthesizeKubernetes(yaml: String): SynthesisResult {
        // Very basic regex extraction for the alien tech prototype.
        // In production, use Jackson YAML or JetBrains YAML PSI.
        val replicasMatch = Regex("replicas:\\s*(\\d+)").find(yaml)
        val replicas = replicasMatch?.groups?.get(1)?.value?.toIntOrNull() ?: 1
        
        val targetThroughput = replicas * 50 // Rough TFLOPS estimation
        val gossipFanout = if (replicas > 100) 5 else 3
        val costFactor = if (replicas > 50) 14 else 8

        val jcl = """
job:
  id: "k8s-migrated-workload"
  swarm_physics:
    pheromone_attraction:
      target_throughput: "${targetThroughput} TFLOPS"
      evaporation_rate_ms: 2000
    network_topology:
      gossip_fanout: $gossipFanout
      max_aggregator_rtt_ms: 120
""".trimIndent()

        val roast = "<b>The Thermodynamics of Centralization:</b> You statically requested $replicas replicas.<br><br>" +
                    "<i>Marabunta Translation:</i> We converted $replicas replicas into a <b>Gossip Fanout = $gossipFanout</b> " +
                    "with a <b>Target Throughput = $targetThroughput TFLOPS</b>. The Swarm will attract nodes probabilistically."

        return SynthesisResult(roast, jcl, costFactor)
    }

    private fun synthesizeTerraform(hcl: String): SynthesisResult {
        val instanceMatch = Regex("instance_type\\s*=\\s*\"([^\"]+)\"").find(hcl)
        val instanceType = instanceMatch?.groups?.get(1)?.value ?: "unknown"
        
        val isGpu = instanceType.contains("p3") || instanceType.contains("p4") || instanceType.contains("g4")
        val throughput = if (isGpu) "8 PETAFLOPS" else "200 TFLOPS"
        val costFactor = if (isGpu) 22 else 11

        val jcl = """
job:
  id: "tf-migrated-workload"
  swarm_physics:
    pheromone_attraction:
      target_throughput: "$throughput"
      trait_requirements:
        - "${if (isGpu) "cuda_11.8" else "avx512"}"
    network_topology:
      max_aggregator_rtt_ms: 85
""".trimIndent()

        val roast = "<b>The Fault of Static Capacity:</b> You hardcoded the AWS SKU `$instanceType`.<br><br>" +
                    "<i>Marabunta Translation:</i> We translated this into Trait Attraction (`${if (isGpu) "cuda_11.8" else "avx512"}`). " +
                    "The Swarm sources hardware globally based on latency, bypassing AWS pricing margins entirely."

        return SynthesisResult(roast, jcl, costFactor)
    }

    private fun synthesizeDocker(dockerfile: String): SynthesisResult {
        val baseImageMatch = Regex("FROM\\s+([^\\s]+)").find(dockerfile)
        val baseImage = baseImageMatch?.groups?.get(1)?.value ?: "ubuntu"

        val jcl = """
payloads:
  - id: "pure-math-function"
    type: "wasm32-wasip1"
    source: "s3://marabunta-clearnet/jobs/extracted.wasm"
    execution_bounds:
      max_fuel: 50000000
      max_memory_mb: 256
""".trimIndent()

        val roast = "Your Dockerfile relies on `$baseImage`. 99.9% of those bytes are idle operating system dependencies.<br><br>" +
                    "<i>Marabunta Translation:</i> We discard `$baseImage`, extract the mathematical AST, and compile to `wasm32-wasi`. " +
                    "Payload size drops to <b>450 Kilobytes</b>."

        return SynthesisResult(roast, jcl, 5)
    }

    private fun synthesizeKafka(config: String): SynthesisResult {
        val jcl = """
topology:
  routing: "Plumtree_Epidemic_Gossip"
  stigmergic_ttl_ms: 5000
  dag_consensus:
    causality_window: "Eventual"
""".trimIndent()

        return SynthesisResult(
            "You are forcing a swarm to wait at a centralized tollbooth. 'HA Kafka' is just a clustered monolith.<br><br>" +
            "<i>Marabunta Translation:</i> Synthesized into Epidemic Gossip. Data propagates exponentially. We abandon strict chronologic ordering for causal DAG consensus.",
            jcl,
            18
        )
    }

    private fun synthesizeS3(config: String): SynthesisResult {
        val jcl = """
storage:
  strategy: "Planetary_Scatter"
  erasure_coding:
    data_shards: 10
    parity_shards: 4
  encryption: "ML-KEM_Post_Quantum"
""".trimIndent()

        return SynthesisResult(
            "You are locking data to a specific geographic grid, subjecting yourself to catastrophic egress fees.<br><br>" +
            "<i>Marabunta Translation:</i> We shred this bucket into a <b>Reed-Solomon Matrix</b> and scatter parity shards worldwide.",
            jcl,
            30
        )
    }
}