// Marabunta - Licensed under the MIT License.
// ── ContextFace ──
// Back face (-Z): Displays context data as key-value pairs in a two-column
// layout. Keys are left-aligned in muted color; values are right-aligned
// in bright text. Supports up to 8 visible pairs; longer contexts show
// a scroll indicator. Values exceeding 25 characters are truncated.
// This face is decomposable.

import { FONT_MONO, truncate } from '../types';
import type { FaceProps, ContextFaceData } from '../types';

// ── Component ──

export function ContextFace({ data, size }: FaceProps<ContextFaceData>) {
  const visible = data.entries.slice(0, 8);
  const overflow = data.totalKeys - visible.length;

  return (
    <group>
      {visible.map((entry, i) => (
        <group key={i} position={[0, size * 0.35 - i * size * 0.09, 0.002]}>
          {/* Key -- left-aligned, muted */}
          <troikaText
            text={entry.key}
            fontSize={size * 0.038}
            font={FONT_MONO}
            color="#64748b"
            anchorX="left"
            anchorY="middle"
            position={[-size * 0.42, 0, 0]}
            maxWidth={size * 0.38}
            sdfGlyphSize={64}
          />
          {/* Value -- right-aligned, bright */}
          <troikaText
            text={truncate(entry.value, 25)}
            fontSize={size * 0.038}
            font={FONT_MONO}
            color="#e8ecf4"
            anchorX="right"
            anchorY="middle"
            position={[size * 0.42, 0, 0]}
            maxWidth={size * 0.42}
            sdfGlyphSize={64}
          />
        </group>
      ))}
      {/* Overflow scroll indicator */}
      {overflow > 0 && (
        <group position={[0, -size * 0.4, 0.002]}>
          <troikaText
            text={`\u2193 ${overflow} more keys`}
            fontSize={size * 0.032}
            font={FONT_MONO}
            color="#475569"
            anchorX="center"
            anchorY="middle"
            sdfGlyphSize={64}
          />
        </group>
      )}
    </group>
  );
}
