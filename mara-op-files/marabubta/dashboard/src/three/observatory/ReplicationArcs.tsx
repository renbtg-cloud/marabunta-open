// Marabunta - Licensed under the MIT License.
// ── ReplicationArcs ──
// Thick curved arcs representing data replication links between nodes.
// QuadraticBezierCurve3 arcs that rise above the node plane.
// Thickness = replication factor, glow = freshness, color shift when stale.

import { useMemo } from 'react';
import * as THREE from 'three';
import { type ReplicationLink, type LayoutEngine } from './types';

interface ReplicationArcsProps {
  links: ReplicationLink[];
  layout: LayoutEngine;
}

/** Build a bezier curve that arcs upward between source and target. */
function buildArc(
  src: THREE.Vector3,
  tgt: THREE.Vector3,
): THREE.QuadraticBezierCurve3 {
  const mid = new THREE.Vector3().lerpVectors(src, tgt, 0.5);
  const dist = src.distanceTo(tgt);
  mid.y += dist * 0.3;
  return new THREE.QuadraticBezierCurve3(src, mid, tgt);
}

/** Single replication arc. */
function ReplicationArc({ link, layout }: { link: ReplicationLink; layout: LayoutEngine }) {
  const { tubeGeo, freshness, baseColor, opacity } = useMemo(() => {
    const srcPos = layout.getPosition(link.sourceId);
    const tgtPos = layout.getPosition(link.targetId);
    const curve = buildArc(srcPos, tgtPos);

    const tubeRadius = 0.02 + link.replicationFactor * 0.015;
    const geo = new THREE.TubeGeometry(curve, 32, tubeRadius, 8, false);

    // Freshness: 1.0 = current, 0.0 = very stale (>60s lag)
    const fresh = Math.max(0, 1.0 - link.lagMs / 60000);
    const color = link.healthy ? '#22d3ee' : '#fb7185';
    const opacityVal = 0.5 + fresh * 0.5;

    return {
      tubeGeo: geo,
      freshness: fresh,
      baseColor: color,
      opacity: opacityVal,
    };
  }, [link, layout]);

  return (
    <mesh geometry={tubeGeo} renderOrder={0}>
      <meshStandardMaterial
        color={baseColor}
        emissive={baseColor}
        emissiveIntensity={freshness * 0.8}
        transparent
        opacity={opacity}
        depthWrite={false}
        side={THREE.DoubleSide}
      />
    </mesh>
  );
}

export function ReplicationArcs({ links, layout }: ReplicationArcsProps) {
  if (links.length === 0) return null;

  return (
    <group renderOrder={0}>
      {links.map((link, i) => (
        <ReplicationArc
          key={`${link.sourceId}-${link.targetId}-${i}`}
          link={link}
          layout={layout}
        />
      ))}
    </group>
  );
}
