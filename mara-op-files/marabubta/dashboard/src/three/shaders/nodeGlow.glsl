// dashboard/src/three/shaders/nodeGlow.glsl
// Custom glow shader for swarm node spheres.
// Combines Phong diffuse shading with:
//   - Sinusoidal pulse animation driven by health state
//   - Fresnel rim glow for halo effect at sphere edges
//   - Additive core glow for selection/prominence
//   - Reinhard tone mapping to prevent HDR blowout
//
// Uniforms are per-mesh in single-node mode.
// For InstancedMesh, uNodeColor/uPulseRate/uGlowIntensity become
// instance attributes (handled by the rendering layer in W5B).

// ===== VERTEX SHADER =====
// #pragma vertex
precision highp float;

uniform float uTime;           // Elapsed time in seconds (from useFrame clock)
uniform float uPulseRate;      // Hz: 0.5 (alive), 2.0 (suspect), 0.0 (dead)
uniform float uGlowIntensity;  // Base glow strength 0.0 - 1.0
uniform vec3  uNodeColor;      // Role-blended color (RGB, linear space)

varying vec3 vNormal;
varying vec3 vWorldPos;
varying float vGlow;

void main() {
    // Transform normal to view space for lighting
    vNormal = normalize(normalMatrix * normal);

    // Compute world position for Fresnel calculation
    vec4 worldPos = modelMatrix * vec4(position, 1.0);
    vWorldPos = worldPos.xyz;

    // Pulse: sinusoidal oscillation based on health rate
    // alive (0.5 Hz) = slow breathing, suspect (2.0 Hz) = rapid warning
    // dead (0.0 Hz) = static (pulse stays at 1.0)
    float pulse = 1.0;
    if (uPulseRate > 0.001) {
        pulse = 0.85 + 0.15 * sin(uTime * uPulseRate * 6.28318);
    }

    // Slight vertex displacement along normal for breathing effect
    // Max displacement is 0.02 world units — subtle but visible
    vec3 displaced = position + normal * 0.02 * (pulse - 0.85) / 0.15;

    // Pass glow intensity (modulated by pulse) to fragment shader
    vGlow = uGlowIntensity * pulse;

    gl_Position = projectionMatrix * viewMatrix * modelMatrix * vec4(displaced, 1.0);
}

// ===== FRAGMENT SHADER =====
// #pragma fragment
precision highp float;

uniform vec3  uNodeColor;       // Role-blended color
uniform float uGlowIntensity;   // Base glow strength
uniform vec3  uLightDir;        // Normalized direction to key light
uniform float uAmbient;         // Ambient intensity (typically 0.4)

varying vec3 vNormal;
varying vec3 vWorldPos;
varying float vGlow;

void main() {
    // ── Phong Diffuse ──
    // Classic N dot L for directional shading
    float NdotL = max(dot(vNormal, uLightDir), 0.0);
    vec3 diffuse = uNodeColor * (uAmbient + (1.0 - uAmbient) * NdotL);

    // ── Fresnel Rim Glow ──
    // Stronger at sphere edges where normal is perpendicular to view
    // Creates a halo effect that the bloom post-process amplifies
    vec3 viewDir = normalize(cameraPosition - vWorldPos);
    float fresnel = pow(1.0 - max(dot(vNormal, viewDir), 0.0), 3.0);
    vec3 rim = uNodeColor * fresnel * vGlow * 1.5;

    // ── Core Glow ──
    // Subtle additive glow across the entire surface
    // Selected nodes (glow=1.0) appear to emit light
    vec3 coreGlow = uNodeColor * vGlow * 0.3;

    // ── Composite ──
    vec3 color = diffuse + rim + coreGlow;

    // ── Tone Mapping ──
    // Simple Reinhard to prevent bloom from blowing out to white
    color = color / (color + vec3(1.0));

    gl_FragColor = vec4(color, 1.0);
}
