// Marabunta - Licensed under the MIT License.
// ── TimersFace ──
// Bottom face (-Y): Displays active timers as countdown values. Each timer
// shows its label, remaining time (HH:MM:SS or Xd Xh), and a progress bar.
// Timers within 10% of expiry pulse with a red glow. Uses useFrame for
// smooth countdown animation without re-rendering the React tree.

import { useRef } from 'react';
import { useFrame } from '@react-three/fiber';
import { FONT_MONO, formatDuration } from '../types';
import type { FaceProps, TimersFaceData } from '../types';

// ── Component ──

export function TimersFace({ data, size }: FaceProps<TimersFaceData>) {
  const textRefs = useRef<Map<number, any>>(new Map());
  const barRefs = useRef<Map<number, any>>(new Map());

  // Update countdown text every frame without React re-render
  useFrame(() => {
    const now = Date.now();
    data.timers.forEach((timer, i) => {
      const textRef = textRefs.current.get(i);
      if (textRef) {
        const remaining = Math.max(
          0,
          (new Date(timer.deadline).getTime() - now) / 1000,
        );
        textRef.text = formatDuration(remaining);

        // Pulse color when within 10% of expiry
        const progress = timer.totalDuration > 0
          ? remaining / timer.totalDuration
          : 0;
        if (progress < 0.1 && remaining > 0) {
          // Pulse between red and normal at ~2Hz
          const pulse = Math.sin(now * 0.004 * Math.PI) * 0.5 + 0.5;
          textRef.color = pulse > 0.5 ? '#fb7185' : '#f43f5e';
        } else {
          textRef.color = '#fb7185';
        }
        textRef.sync();
      }

      // Update progress bar width
      const barRef = barRefs.current.get(i);
      if (barRef && timer.totalDuration > 0) {
        const remaining = Math.max(
          0,
          (new Date(timer.deadline).getTime() - now) / 1000,
        );
        const progress = remaining / timer.totalDuration;
        barRef.scale.x = Math.max(0.001, progress);
      }
    });
  });

  return (
    <group>
      {data.timers.slice(0, 4).map((timer, i) => (
        <group key={i} position={[0, size * 0.25 - i * size * 0.16, 0.002]}>
          {/* Timer label */}
          <troikaText
            text={timer.label}
            fontSize={size * 0.035}
            font={FONT_MONO}
            color="#64748b"
            anchorX="left"
            anchorY="middle"
            position={[-size * 0.4, size * 0.025, 0]}
            maxWidth={size * 0.5}
            sdfGlyphSize={64}
          />
          {/* Countdown value */}
          <troikaText
            ref={(el: any) => {
              if (el) textRefs.current.set(i, el);
            }}
            text={formatDuration(timer.remaining)}
            fontSize={size * 0.06}
            font={FONT_MONO}
            color="#fb7185"
            anchorX="right"
            anchorY="middle"
            position={[size * 0.4, 0.01, 0]}
            sdfGlyphSize={64}
          />
          {/* Progress bar background */}
          <mesh position={[0, -size * 0.035, 0]}>
            <planeGeometry args={[size * 0.8, size * 0.015]} />
            <meshStandardMaterial color="#1e293b" transparent opacity={0.6} />
          </mesh>
          {/* Progress bar fill */}
          <mesh
            ref={(el: any) => {
              if (el) barRefs.current.set(i, el);
            }}
            position={[0, -size * 0.035, 0.001]}
          >
            <planeGeometry args={[size * 0.8, size * 0.015]} />
            <meshStandardMaterial
              color="#fb7185"
              emissive="#fb7185"
              emissiveIntensity={0.15}
              transparent
              opacity={0.6}
            />
          </mesh>
        </group>
      ))}
    </group>
  );
}
