// Marabunta - Licensed under the MIT License.
// ── FaceContent ──
// Abstract face content dispatcher. Given a FaceType and CubeData, renders
// the correct face component (StateFace, ActorsFace, etc.).

import { StateFace } from './faces/StateFace';
import { ActorsFace } from './faces/ActorsFace';
import { AuditFace } from './faces/AuditFace';
import { ContextFace } from './faces/ContextFace';
import { InfraFace } from './faces/InfraFace';
import { TimersFace } from './faces/TimersFace';
import type { FaceType, CubeData } from './types';

interface FaceContentProps {
  type: FaceType;
  data: CubeData;
  size: number;
}

export function FaceContent({ type, data, size }: FaceContentProps) {
  switch (type) {
    case 'state':
      return <StateFace data={data.state} size={size} />;
    case 'actors':
      return <ActorsFace data={data.actors} size={size} />;
    case 'audit':
      return <AuditFace data={data.audit} size={size} />;
    case 'context':
      return <ContextFace data={data.context} size={size} />;
    case 'infra':
      return <InfraFace data={data.infra} size={size} />;
    case 'timers':
      return <TimersFace data={data.timers} size={size} />;
    case 'custom':
      // Custom face rendering is handled by external renderers
      // registered in later waves. Show placeholder for now.
      return (
        <troikaText
          text="CUSTOM"
          fontSize={size * 0.06}
          color="#64748b"
          anchorX="center"
          anchorY="middle"
          position={[0, 0, 0.002]}
        />
      );
    default:
      return null;
  }
}
