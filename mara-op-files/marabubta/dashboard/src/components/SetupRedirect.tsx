// Marabunta - Licensed under the MIT License.
import { Navigate } from 'react-router-dom';
import { useSetupStatus } from '../hooks/useSetupStatus';

interface Props {
  children: React.ReactNode;
}

export function SetupRedirect({ children }: Props) {
  const { data: status, isLoading } = useSetupStatus();

  if (isLoading) return null;

  if (status?.is_first_run) {
    return <Navigate to="/setup" replace />;
  }

  return <>{children}</>;
}
