// Marabunta - Licensed under the MIT License.
package com.marabunta.rover

// Stage 5.2: Behavioral Policy Shaping
// Replaces live LLM generation with deterministic threshold rules that train an offline model.

class BehavioralPolicyManager {

    private val userDefinedRules = mutableListOf<RoutingRule>()

    // Users define strict physical parameters for when jobs should be rerouted
    data class RoutingRule(
        val metric: String,
        val operator: String,
        val threshold: Double,
        val targetTier: String
    )

    fun addRule(metric: String, operator: String, threshold: Double, targetTier: String) {
        userDefinedRules.add(RoutingRule(metric, operator, threshold, targetTier))
        println("Added deterministic rule: IF \$metric \$operator \$threshold THEN route to \$targetTier")
    }

    fun evaluateTelemetry(edgeRamPct: Double, mmxPrice: Double): String? {
        // Deterministic execution replaces live LLM hallucination
        for (rule in userDefinedRules) {
            val conditionMet = when (rule.metric) {
                "edge_ram" -> evaluateCondition(edgeRamPct, rule.operator, rule.threshold)
                "spot_price" -> evaluateCondition(mmxPrice, rule.operator, rule.threshold)
                else -> false
            }
            if (conditionMet) {
                logBehaviorForOfflineTraining(rule)
                return rule.targetTier
            }
        }
        return null // Proceed with default topology
    }

    private fun evaluateCondition(actual: Double, operator: String, threshold: Double): Boolean {
        return when (operator) {
            ">" -> actual > threshold
            "<" -> actual < threshold
            "==" -> actual == threshold
            else -> false
        }
    }

    private fun logBehaviorForOfflineTraining(rule: RoutingRule) {
        // Appends to a local append-only log that is periodically sent to the training cluster
        println("Logging routing decision for offline Behavioral Model fine-tuning: \$rule")
    }
}
