// Marabunta - Licensed under the MIT License.
package com.marabunta.rover

import com.intellij.notification.NotificationGroupManager
import com.intellij.notification.NotificationType
import com.intellij.ui.components.JBCheckBox
import com.intellij.ui.components.JBScrollPane
import com.intellij.util.ui.JBUI
import java.awt.BorderLayout
import java.awt.Color
import java.awt.Font
import java.awt.Toolkit
import java.awt.datatransfer.StringSelection
import javax.swing.*

class TopologicalWaiverPanel(
    private val title: String,
    private val roastHtml: String,
    private val synthesizedJcl: String,
    private val waiverText: String
) : JPanel(BorderLayout()) {

    private val jclTextArea = JTextArea().apply {
        text = "

      [ L O C K E D ] 

      Acknowledge Swarm Physics to reveal synthesized JCL."
        font = Font("Monospaced", Font.ITALIC, 12)
        isEditable = false
        foreground = Color.GRAY
        background = JBUI.CurrentTheme.EditorTabs.background()
    }

    private val copyButton = JButton("📋 Copy Synthesized JCL").apply {
        isEnabled = false
        addActionListener {
            val clipboard = Toolkit.getDefaultToolkit().systemClipboard
            clipboard.setContents(StringSelection(synthesizedJcl), null)
            
            // The "Classy" Evasion Notification
            try {
                NotificationGroupManager.getInstance()
                    .getNotificationGroup("Marabunta Swarm Events")
                    .createNotification(
                        "JCL Copied to Clipboard",
                        "Proceed with caution. The Swarm routes probabilistically. You own this execution topology.",
                        NotificationType.WARNING
                    )
                    .notify(null)
            } catch (e: Exception) {
                // Fallback for missing notification group config in plugin.xml during dev
            }
        }
    }

    private val waiverCheckbox = JBCheckBox("<html>$waiverText</html>").apply {
        foreground = JBUI.CurrentTheme.Label.errorForeground()
        addActionListener {
            val isAcknowledged = this.isSelected
            copyButton.isEnabled = isAcknowledged
            
            if (isAcknowledged) {
                jclTextArea.text = synthesizedJcl
                jclTextArea.foreground = JBUI.CurrentTheme.Label.foreground()
                jclTextArea.font = Font("Monospaced", Font.PLAIN, 12)
            } else {
                jclTextArea.text = "

      [ L O C K E D ] 

      Acknowledge Swarm Physics to reveal synthesized JCL."
                jclTextArea.foreground = Color.GRAY
                jclTextArea.font = Font("Monospaced", Font.ITALIC, 12)
            }
        }
    }

    init {
        // The Roast / Educational Header
        val headerPanel = JPanel(BorderLayout()).apply {
            border = BorderFactory.createEmptyBorder(10, 10, 10, 10)
            add(JLabel("<html><b>$title</b><br><br>$roastHtml</html>"), BorderLayout.NORTH)
        }

        // The Waiver Footer
        val footerPanel = JPanel(BorderLayout()).apply {
            border = BorderFactory.createEmptyBorder(10, 10, 10, 10)
            add(waiverCheckbox, BorderLayout.NORTH)
            add(Box.createVerticalStrut(10), BorderLayout.CENTER)
            add(copyButton, BorderLayout.SOUTH)
        }

        add(headerPanel, BorderLayout.NORTH)
        add(JBScrollPane(jclTextArea), BorderLayout.CENTER)
        add(footerPanel, BorderLayout.SOUTH)
    }
}