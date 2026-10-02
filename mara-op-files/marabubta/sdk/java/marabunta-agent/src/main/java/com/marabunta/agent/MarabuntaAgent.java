// Marabunta - Licensed under the MIT License.
package com.marabunta.agent;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.instrument.Instrumentation;

public class MarabuntaAgent {

    public static void premain(String arg, Instrumentation inst) {
        System.out.println("[MARABUNTA SYMBIONT] Java Agent Attached.");
        System.out.println("[MARABUNTA SYMBIONT] Scanning for @SwarmOffload annotations and heuristic bottlenecks...");

        new AgentBuilder.Default()
            // Narrow down to application classes (avoid instrumenting java.lang etc)
            .type(ElementMatchers.nameStartsWith("com.yourcompany").or(ElementMatchers.nameMatches(".*Service.*|.*Processor.*")))
            .transform((builder, typeDescription, classLoader, module, protectionDomain) ->
                builder.method(
                        // Intercept explicitly annotated methods
                        ElementMatchers.isAnnotatedWith(ElementMatchers.nameEndsWith("SwarmOffload"))
                        // OR intercept heuristic long-running linear methods
                        .or(ElementMatchers.nameMatches(".*processBatch.*|.*calculateMassive.*|.*generateReport.*"))
                       )
                       .intercept(MethodDelegation.to(SwarmInterceptor.class))
            )
            .installOn(inst);
    }
}
