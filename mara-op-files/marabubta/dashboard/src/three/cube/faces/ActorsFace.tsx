// Marabunta - Licensed under the MIT License.
// ── ActorsFace ──
// Top face (+Y): Shows assigned actors as a vertical list. Each entry
// includes the actor's name, role badge, and active indicator. If more
// than 4 actors are assigned, the face shows the first 3 plus "+N more".
// Actor names are truncated with ellipsis at 20 characters.

import { FONT_MONO, FONT_SANS, truncate } from '../types';
import type { FaceProps, ActorsFaceData } from '../types';

// ── Component ──

export function ActorsFace({ data, size }: FaceProps<ActorsFaceData>) {
  const visible = data.actors.slice(0, 3);
  const overflow = data.actors.length - 3;

  return (
    <group>
      {visible.map((actor, i) => (
        <group key={i} position={[0, size * 0.25 - i * size * 0.18, 0.002]}>
          {/* Active indicator dot */}
          <mesh position={[-size * 0.43, 0, 0]}>
            <circleGeometry args={[size * 0.012, 16]} />
            <meshStandardMaterial
              color={actor.active ? '#34d399' : '#475569'}
              emissive={actor.active ? '#34d399' : '#000000'}
              emissiveIntensity={actor.active ? 0.5 : 0}
            />
          </mesh>
          {/* Actor name */}
          <troikaText
            text={truncate(actor.name, 20)}
            fontSize={size * 0.055}
            font={FONT_SANS}
            color={actor.active ? '#e8ecf4' : '#64748b'}
            anchorX="left"
            anchorY="middle"
            position={[-size * 0.4, 0, 0]}
            maxWidth={size * 0.55}
            sdfGlyphSize={64}
          />
          {/* Role badge */}
          <troikaText
            text={actor.role}
            fontSize={size * 0.035}
            font={FONT_MONO}
            color="#a78bfa"
            anchorX="right"
            anchorY="middle"
            position={[size * 0.4, 0, 0]}
            sdfGlyphSize={64}
          />
        </group>
      ))}
      {/* Overflow indicator */}
      {overflow > 0 && (
        <troikaText
          text={`+${overflow} more`}
          fontSize={size * 0.04}
          font={FONT_MONO}
          color="#64748b"
          anchorX="center"
          anchorY="middle"
          position={[0, -size * 0.35, 0.002]}
          sdfGlyphSize={64}
        />
      )}
    </group>
  );
}
