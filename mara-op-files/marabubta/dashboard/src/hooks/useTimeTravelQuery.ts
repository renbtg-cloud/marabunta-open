// Marabunta - Licensed under the MIT License.
import { useQuery } from '@tanstack/react-query';
import { useTimeTravel } from './useTimeTravel';
import { api } from '../api/client';

/**
 * Wraps React Query's useQuery with time-travel awareness.
 *
 * When time-travel is active (viewing historical data):
 * - Appends `as_of` query parameter to the API call
 * - Disables refetch interval (historical data doesn't change)
 * - Sets staleTime to Infinity
 *
 * When live: normal React Query behavior.
 */
export function useTimeTravelQuery<T>(
  baseKey: string[],
  path: string,
  params?: Record<string, string>,
  options?: {
    staleTime?: number;
    refetchInterval?: number;
    enabled?: boolean;
  },
) {
  const { isLive, timestamp } = useTimeTravel();

  const queryParams: Record<string, string> = { ...params };
  if (!isLive && timestamp) {
    queryParams.as_of = timestamp;
  }

  return useQuery({
    queryKey: [...baseKey, isLive ? 'live' : timestamp],
    queryFn: () => api.get<T>(path, queryParams),
    staleTime: isLive ? (options?.staleTime ?? 10_000) : Infinity,
    refetchInterval: isLive ? (options?.refetchInterval ?? undefined) : undefined,
    enabled: options?.enabled ?? true,
  });
}
