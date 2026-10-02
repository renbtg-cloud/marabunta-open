// Marabunta - Licensed under the MIT License.
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { Layout } from './components/Layout';
import { SwarmOverview } from './pages/SwarmOverview';
import { Setup } from './pages/Setup';
import { SetupRedirect } from './components/SetupRedirect';
import { NodeDetail } from './pages/NodeDetail';
import { FleetOverview } from './pages/FleetOverview';
import { StorageTierStatus } from './pages/StorageTierStatus';
import { AuditTrailExplorer } from './pages/AuditTrailExplorer';
import { Settings } from './pages/Settings';

import { WarGames } from './pages/WarGames';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 2,
      refetchOnWindowFocus: false,
      staleTime: 10_000,
    },
  },
});

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route element={<Layout />}>
            <Route path="/" element={<SetupRedirect><SwarmOverview /></SetupRedirect>} />
            <Route path="/setup" element={<Setup />} />
            <Route path="/nodes/:id" element={<NodeDetail />} />
            <Route path="/fleet" element={<FleetOverview />} />
            <Route path="/storage" element={<StorageTierStatus />} />
            <Route path="/audit" element={<AuditTrailExplorer />} />
            <Route path="/settings" element={<Settings />} />
            <Route path="/wargames" element={<WarGames />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
