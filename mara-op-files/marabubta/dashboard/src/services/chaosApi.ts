// Marabunta - Licensed under the MIT License.
// Types for Chaos Engineering
export type DeathType =
  | { NetworkPartition: { duration: { secs: number, nanos: number } | null } }
  | { ByzantineCorruption: { duration: { secs: number, nanos: number } | null } }
  | { ThermalPanic: { duration: { secs: number, nanos: number } | null } }
  | { JitterStorm: { duration: { secs: number, nanos: number } | null } }
  | 'FatalCrash';

const API_BASE_URL = 'http://127.0.0.1:8080/api/v1';

export const issueChaosStrike = async (
    target_nodes: string[],
    target_region: string | null,
    death_type: DeathType,
) => {
    const res = await fetch(`${API_BASE_URL}/chaos/strike`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ target_nodes, target_region, death_type })
    });
    
    if (!res.ok) {
        throw new Error(`Failed to issue strike: ${await res.text()}`);
    }
    return res.json();
};

export const issueChaosRevive = async (
    target_nodes: string[],
    target_region: string | null,
) => {
    const res = await fetch(`${API_BASE_URL}/chaos/revive`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ target_nodes, target_region })
    });
    
    if (!res.ok) {
        throw new Error(`Failed to issue revive: ${await res.text()}`);
    }
    return res.json();
};
