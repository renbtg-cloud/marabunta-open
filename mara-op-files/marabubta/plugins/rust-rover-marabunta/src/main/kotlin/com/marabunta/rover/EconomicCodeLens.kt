// Marabunta - Licensed under the MIT License.
package com.marabunta.rover

import com.intellij.codeInsight.daemon.LineMarkerInfo
import com.intellij.codeInsight.daemon.LineMarkerProvider
import com.intellij.openapi.editor.markup.GutterIconRenderer
import com.intellij.psi.PsiElement
import com.intellij.util.PlatformIcons
import java.awt.event.MouseEvent

class EconomicCodeLens : LineMarkerProvider {

    override fun getLineMarkerInfo(element: PsiElement): LineMarkerInfo<*>? {
        // In a production environment, we traverse the PSI tree to find function declarations 
        // annotated with @workflow or specific JCL YAML blocks.
        // For the alien tech prototype, we target specific leaf nodes.
        val text = element.text ?: return null
        
        val isTarget = text == "@workflow" || text == "job:" || text.startsWith("mesh://")

        if (!isTarget) return null
        
        // Connect to Phase 1 Telemetry Firehose (Mocked here)
        val swarmSpotPriceUsd = 0.02
        val awsEquivalentUsd = 0.85
        
        return LineMarkerInfo(
            element,
            element.textRange,
            PlatformIcons.WEB_ICON, // Placeholder for Marabunta Swarm Icon
            { "💡 MMX Spot Market Projection: \$${swarmSpotPriceUsd}/hr (AWS: \$${awsEquivalentUsd}/hr)" },
            { _: MouseEvent, _: PsiElement -> 
                // TODO: Open Market Override Slider to set GIVEN(max_bid_usd)
            },
            GutterIconRenderer.Alignment.RIGHT,
            { "Swarm Economics" }
        )
    }
}