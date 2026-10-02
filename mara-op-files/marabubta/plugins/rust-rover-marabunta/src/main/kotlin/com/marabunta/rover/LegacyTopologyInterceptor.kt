// Marabunta - Licensed under the MIT License.
package com.marabunta.rover

import com.intellij.openapi.fileEditor.FileEditor
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.DialogBuilder
import com.intellij.openapi.vfs.VirtualFile
import com.intellij.ui.EditorNotificationPanel
import com.intellij.ui.EditorNotificationProvider
import java.util.function.Function
import javax.swing.JComponent

class LegacyTopologyInterceptor : EditorNotificationProvider {

    override fun collectNotificationData(
        project: Project,
        file: VirtualFile
    ): Function<in FileEditor, out JComponent?>? {

        val fileType = identifyLegacyAntiPattern(file) ?: return null

        return Function { fileEditor ->
            val panel = EditorNotificationPanel()
            panel.text = "⚠️ Legacy Deterministic Topology Detected. This workload will cost 11x more and suffer from 4% network drop failures."
            
            panel.createActionLabel("⟁ Translate to Swarm JCL") {
                showWaiverDialog(project, fileEditor, fileType)
            }
            panel
        }
    }

    private fun identifyLegacyAntiPattern(file: VirtualFile): AntiPatternType? {
        val name = file.name.lowercase()
        val ext = file.extension?.lowercase()

        if (name.contains("dockerfile") || name == "docker-compose.yml") {
            return AntiPatternType.DOCKER
        }
        if (ext == "tf") {
            return AntiPatternType.TERRAFORM
        }
        if (name.contains("kafka") || name.contains("queue") || name.contains("rabbitmq")) {
            return AntiPatternType.KAFKA
        }
        if (name.contains("pvc") || name.contains("s3")) {
            return AntiPatternType.S3
        }
        
        // Basic K8s detector
        if (ext == "yaml" || ext == "yml") {
            if (name.contains("deployment") || name.contains("pod") || name.contains("statefulset")) {
                return AntiPatternType.KUBERNETES
            }
        }

        return null
    }

    private fun showWaiverDialog(project: Project, fileEditor: FileEditor, antiPatternType: AntiPatternType) {
        val document = (fileEditor as? com.intellij.openapi.fileEditor.TextEditor)?.editor?.document
            ?: return

        val synthesis = TopologySynthesizer.synthesize(antiPatternType, document)

        val dialog = DialogBuilder(project)
        dialog.title("Marabunta: Topological Synthesis (Read-Only)")
        
        val title = when (antiPatternType) {
            AntiPatternType.TERRAFORM, AntiPatternType.KUBERNETES -> "The Fault of Static Capacity"
            AntiPatternType.DOCKER -> "The Bloat of OS-Level Virtualization"
            AntiPatternType.KAFKA -> "The Bottleneck of Centralized Queues"
            AntiPatternType.S3 -> "The Myth of Data Gravity"
        }
        
        val waiverText = when (antiPatternType) {
            AntiPatternType.TERRAFORM, AntiPatternType.KUBERNETES -> "I acknowledge that Marabunta routing is probabilistic. I accept full architectural responsibility for this JCL. I understand geographic constraints are legacy friction."
            AntiPatternType.DOCKER -> "I acknowledge my 1.2GB container was an artifact of my inability to isolate pure functions. I accept this WASM payload and will not use arbitrary Linux kernel syscalls."
            AntiPatternType.KAFKA -> "I accept that the universe is asynchronous. I relinquish the illusion of strict, centralized chronological ordering."
            AntiPatternType.S3 -> "I acknowledge that my data is about to be mathematically shredded and scattered across thousands of untrusted, ephemeral edge nodes."
        }

        val waiverPanel = TopologicalWaiverPanel(
            title = title,
            roastHtml = synthesis.roastHtml + "<br><br><i>Cost Reduction Factor: ${synthesis.costReductionFactor}x</i>",
            synthesizedJcl = synthesis.synthesizedJcl,
            waiverText = waiverText
        )

        dialog.setCenterPanel(waiverPanel)
        dialog.removeAllActions() // Remove default OK/Cancel so they must use the Panel's UI
        dialog.addCloseButton()
        dialog.show()
    }

    enum class AntiPatternType {
        TERRAFORM, KUBERNETES, DOCKER, KAFKA, S3
    }
}