// Marabunta - Licensed under the MIT License.
// ── CubeFace ──
// Positions a troika text group at the correct offset and rotation for a
// given face position on the Inspection Cube. Renders the face label and
// delegates content to FaceContent.

import { Text } from 'troika-three-text';
import { extend } from '@react-three/fiber';
import type { Object3DNode } from '@react-three/fiber';
import { FaceContent } from './FaceContent';
import {
  FACE_POSITIONS,
  FACE_ROTATIONS,
  FONT_MONO,
  OPERATIONAL_PRESET,
} from './types';
import type { FacePosition, FaceType, CubeData } from './types';

// Register troika Text as a React Three Fiber element
extend({ TroikaText: Text });

declare module '@react-three/fiber' {
  interface ThreeElements {
    troikaText: Object3DNode<Text, typeof Text>;
  }
}

// ── Props ──

interface CubeFaceProps {
  facePosition: FacePosition;
  faceType: FaceType;
  data: CubeData;
  size: number;
  onClick?: (face: FacePosition) => void;
  onRightClick?: (face: FacePosition) => void;
}

// ── Accent Color Lookup ──

function getAccentColor(faceType: FaceType): string {
  const config = OPERATIONAL_PRESET.faceConfigs.find(
    (c) => c.type === faceType,
  );
  return config?.accentColor ?? '#64748b';
}

// ── Component ──

export function CubeFace({
  facePosition,
  faceType,
  data,
  size,
  onClick,
  onRightClick,
}: CubeFaceProps) {
  const offset = size / 2 + 0.001; // slight offset to avoid z-fighting
  const rotation = FACE_ROTATIONS[facePosition];
  const position = FACE_POSITIONS[facePosition](offset);
  const accentColor = getAccentColor(faceType);

  const handleClick = (e: any) => {
    e.stopPropagation?.();
    onClick?.(facePosition);
  };

  const handleContextMenu = (e: any) => {
    e.stopPropagation?.();
    e.nativeEvent?.preventDefault?.();
    onRightClick?.(facePosition);
  };

  return (
    <group
      position={position}
      rotation={rotation}
      onClick={handleClick}
      onContextMenu={handleContextMenu}
    >
      {/* Face label — top-left corner */}
      <troikaText
        text={faceType.toUpperCase()}
        font={FONT_MONO}
        fontSize={size * 0.06}
        color={accentColor}
        anchorX="left"
        anchorY="top"
        position={[-size * 0.45, size * 0.45, 0.001]}
        outlineWidth={0.002}
        sdfGlyphSize={64}
      />
      {/* Face content — delegated to FaceContent */}
      <FaceContent
        type={faceType}
        data={data}
        size={size}
      />
    </group>
  );
}
