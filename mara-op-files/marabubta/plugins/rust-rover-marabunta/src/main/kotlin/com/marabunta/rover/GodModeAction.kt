// Marabunta - Licensed under the MIT License.
package com.marabunta.rover

import com.intellij.openapi.actionSystem.AnAction
import com.intellij.openapi.actionSystem.AnActionEvent
import com.intellij.openapi.ui.Messages

class GodModeAction : AnAction("Enter Swarm God-Mode") {
    override fun actionPerformed(e: AnActionEvent) {
        Messages.showInfoMessage(
            "Visual Traffic Shaper Active.

" +
            "Left-Click nodes to Inspect.
" +
            "Right-Click links to Sever/Inject Latency.
" +
            "Drag chunks to Force Re-routing.",
            "Marabunta God-Mode"
        )
    }
}
