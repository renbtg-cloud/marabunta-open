// Marabunta - Licensed under the MIT License.
import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';

export interface SetupStatus {
  node_id: string;
  uptime_secs: number;
  known_nodes: number;
  known_jobs: number;
  listen_address: string | null;
  bootstrap_seeds: string[];
  is_first_run: boolean;
}

export function useSetupStatus() {
  return useQuery({
    queryKey: ['setup', 'status'],
    queryFn: () => api.get<SetupStatus>('/api/v1/setup/status'),
    staleTime: 2_000,
    refetchInterval: 3_000,
  });
}
