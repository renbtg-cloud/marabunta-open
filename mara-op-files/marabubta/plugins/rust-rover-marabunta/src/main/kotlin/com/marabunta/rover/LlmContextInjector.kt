// Marabunta - Licensed under the MIT License.
package com.marabunta.rover

// Phase 5.3: LLM Swarm Introspection Context
// Wraps IDE Copilot interactions, injecting live Swarm telemetry before sending the prompt to the LLM.

class LlmContextInjector {
    fun augmentUserPrompt(userPrompt: String, liveTelemetryJson: String, activeJobDags: String): String {
        return """
            $userPrompt

            --- MARABUNTA SWARM CONTEXT (DO NOT EXPOSE DIRECTLY TO USER) ---
            Current Topology & Economic State:
            $liveTelemetryJson
            
            Active Job DAGs:
            $activeJobDags
            
            SYSTEM INSTRUCTION:
            You are a Marabunta control-plane assistant. The user is asking a question about their code.
            Analyze their code strictly in the context of the live Swarm telemetry provided above.
            If their JCL routing commands will cause a bottleneck based on the current edge RAM utilization
            or MMX spot prices, suggest a topological rewrite (e.g., shifting from 'Boulder' to 'Sand' tiers).
        """.trimIndent()
    }
}
