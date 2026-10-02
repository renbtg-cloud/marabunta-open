// Marabunta - Licensed under the MIT License.
// marabunta-compute/dashboard/src/pages/SystemStatusPage.tsx
import React, { useState, useEffect } from 'react';
import { StatusIcon } from '../components/StatusIcon';
import { SwarmPsyche, PsycheLevel, ZoneHealth, ZoneStatus } from '../services/apiTypes';
import '../styles/SystemStatus.css';

const getLevelName = (level: PsycheLevel) => {
  switch (level) {
    case 1: return 'CRITICAL';
    case 2: return 'STRESSED';
    case 3: return 'NORMAL';
    case 4: return 'HEALTHY';
    case 5: return 'THRIVING';
    default: return 'UNKNOWN';
  }
};

const mapLevelToStatusIcon = (level: PsycheLevel): 'ok' | 'warning' | 'critical' | 'info' => {
    switch (level) {
        case 1: return 'critical';
        case 2: return 'warning'; // Stressed
        case 3: return 'ok';     // Normal
        case 4: return 'ok';     // Healthy
        case 5: return 'ok';     // Thriving
        default: return 'info';
    }
};

const mapZoneStatusToStatusIcon = (status: ZoneStatus): 'ok' | 'warning' | 'critical' | 'info' => {
    switch (status) {
        case 'OPERATIONAL': return 'ok';
        case 'DEGRADED': return 'warning';
        case 'CRITICAL': return 'critical';
        case 'ISOLATED': return 'critical';
        default: return 'info';
    }
};

const mapLogLevelToStatusIcon = (level: string): 'ok' | 'warning' | 'critical' | 'info' => {
    switch (level) {
        case 'INFO': return 'info';
        case 'WARN': return 'warning';
        case 'CRITICAL': return 'critical';
        default: return 'info';
    }
};

// Mock Data
const systemState = {
    powerGridState: 'STABLE',
    medicalDeviceNetwork: 'OK',
    globalUptime: '351d 11h 04m',
};

const recentEvents = [
  { id: 1, time: '2026-02-15 22:10:01Z', level: 'INFO', message: 'Node n-us-east-1a-043 successfully joined swarm.' },
  { id: 2, time: '2026-02-15 22:09:45Z', level: 'WARNING', message: 'Zone eu-west resource utilization > 90% for 15 min.' },
  { id: 3, time: '2026-02-15 22:09:12Z', level: 'CRITICAL', message: 'Medical device network segment d-alpha-001 reported STOPPED.' },
  { id: 5, time: '2026-02-15 22:08:10Z', level: 'INFO', message: 'Configuration change applied by admin-sys.' },
];

export function SystemStatusPage() {
  const [psyche, setPsyche] = useState<SwarmPsyche | null>(null);
  const [zones, setZones] = useState<ZoneHealth[]>([]);

  useEffect(() => {
    const fetchData = async () => {
      setZones(zoneData.zones);
    };
    fetchData();
    const interval = setInterval(fetchData, 5000); // Refresh every 5 seconds
    return () => clearInterval(interval);
  }, []);

  const overallSystemLevel: PsycheLevel = psyche ? Math.min(...Object.values(psyche.facets).map(f => f.level)) as PsycheLevel : 3; // Default to Normal
  const overallStatusText = getLevelName(overallSystemLevel);
  const overallStatusIcon = mapLevelToStatusIcon(overallSystemLevel);

  return (
    <div className="status-grid">
      <div className={`status-banner status-${overallStatusText.toLowerCase()}`}>
        <span className="status-title"><StatusIcon status={overallStatusIcon} size={28} />SYSTEM STATUS</span>
        <span className="status-value">{overallStatusText}</span>
      </div>
      
      {/* Swarm Psyche Panel */}
      {psyche && (
        <div className="psyche-panel">
          <h3>SWARM PSYCHE // <span style={{color: 'var(--text-secondary)'}}>{psyche.archetype}</span></h3>
          <p style={{color: 'var(--text-secondary)', marginTop: '-0.5rem', marginBottom: '1rem', fontSize: '0.9em'}}>{psyche.description}</p>
          <div className="psyche-facets">
            {Object.entries(psyche.facets).map(([name, facet]) => (
              <div key={name} className="psyche-facet-item">
                <StatusIcon status={mapLevelToStatusIcon(facet.level)} size={16} />
                <span className="facet-label">{name.toUpperCase()}</span>
                <span className="facet-value" style={{ color: `var(--color-${mapLevelToStatusIcon(facet.level)})` }}>
                  {getLevelName(facet.level)} {facet.trend === 'rising' ? '▲' : facet.trend === 'falling' ? '▼' : ''}
                </span>
                <div className="facet-score-bar-container">
                  <div className="facet-score-bar" style={{ width: `${facet.score}%`, backgroundColor: `var(--color-${mapLevelToStatusIcon(facet.level)})` }}></div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Zone Overview Panel */}
      {zones.length > 0 && (
        <div className="zone-overview-panel">
          <h3>OPERATIONAL ZONE OVERVIEW</h3>
          <div className="zone-grid">
            {zones.map(zone => (
              <div key={zone.id} className="zone-card">
                <h4><StatusIcon status={mapZoneStatusToStatusIcon(zone.status)} size={18} />{zone.name.toUpperCase()} <span style={{color: 'var(--text-secondary)', fontSize: '0.8em', marginLeft: '0.5em'}}>({zone.id})</span></h4>
                <div className="zone-details">
                  <p>Status: <span style={{color: `var(--color-${mapZoneStatusToStatusIcon(zone.status)})`}}>{zone.status}</span></p>
                  <p>Active Nodes: {zone.activeNodes}</p>
                  {zone.criticalAlerts > 0 && (
                    <p className="critical-alerts">Critical Alerts: {zone.criticalAlerts}</p>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="card-grid">
        <div className="status-card">
          <h2 className="card-title">Active Alerts</h2>
          <p className={`card-value-large ${systemState.activeAlerts > 0 ? 'text-critical' : 'text-ok'}`}>
            <StatusIcon status={systemState.activeAlerts > 0 ? 'critical' : 'ok'} size={24} />
            {systemState.activeAlerts}
          </p>
        </div>
        <div className="status-card">
          <h2 className="card-title">Global Uptime</h2>
          <p className="card-value-large">{systemState.globalUptime}</p>
        </div>
        <div className="subsystem-card">
          <h3>Power Grid Control</h3>
          <p className={`subsystem-status status-value-${systemState.powerGridState.toLowerCase()}`}>
            <StatusIcon status={mapSubsystemStatusToLevel(systemState.powerGridState)} size={24} />
            {systemState.powerGridState}
          </p>
        </div>
        <div className="subsystem-card">
          <h3>Medical Device Network</h3>
          <p className={`subsystem-status status-value-${systemState.medicalDeviceNetwork.toLowerCase()}`}>
            <StatusIcon status={mapSubsystemStatusToLevel(systemState.medicalDeviceNetwork)} size={24} />
            {systemState.medicalDeviceNetwork}
          </p>
        </div>
      </div>

      <div className="event-log-panel">
        <h3>RECENT OPERATIONAL EVENTS</h3>
        <table>
          <thead>
            <tr>
              <th>Timestamp</th>
              <th>Level</th>
              <th>Message</th>
            </tr>
          </thead>
          <tbody>
            {recentEvents.map(event => (
              <tr key={event.id}>
                <td className="text-secondary">{event.time}</td>
                <td>
                  <span className={`log-level log-${event.level.toLowerCase()}`}>
                    <StatusIcon status={mapLogLevelToStatusIcon(event.level)} size={12} />
                    {event.level}
                  </span>
                </td>
                <td>{event.message}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
