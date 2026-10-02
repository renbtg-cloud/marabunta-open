// Marabunta - Licensed under the MIT License.
package com.marabunta.rover

import com.intellij.lang.Language
import com.intellij.lang.injection.MultiHostInjector
import com.intellij.lang.injection.MultiHostRegistrar
import com.intellij.openapi.util.TextRange
import com.intellij.psi.PsiElement
import com.intellij.psi.PsiLanguageInjectionHost
import com.intellij.psi.xml.XmlText

// Phase 2.2: Live Schema & Topology Validation (Language Injection)
// Injects JCL Language into arbitrary host strings across the workspace.
class JclLanguageInjector : MultiHostInjector {
    override fun getLanguagesToInject(registrar: MultiHostRegistrar, context: PsiElement) {
        if (context is PsiLanguageInjectionHost && context.text.contains("MAP ") && context.text.contains("PULL FROM STREAM")) {
            val language = Language.findLanguageByID("JCL") ?: return
            
            // Inject the JCL grammar directly into the host language string literal
            registrar.startInjecting(language)
                .addPlace(null, null, context, TextRange(1, context.textLength - 1))
                .doneInjecting()
        }
    }

    override fun elementsToInjectIn(): List<Class<out PsiElement>> {
        return listOf(PsiLanguageInjectionHost::class.java)
    }
}
