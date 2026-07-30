import { createContext, ReactNode, useCallback, useContext } from "react";

interface ClusterState {
  available: never[];
  installed: never[];
  loading: boolean;
}

interface ClusterContextValue extends ClusterState {
  refresh: () => Promise<void>;
}

const ClusterContext = createContext<ClusterContextValue | null>(null);

export function ClusterProvider({ children }: { children: ReactNode }) {
  const refresh = useCallback(async () => {}, []);

  return (
    <ClusterContext.Provider
      value={{ available: [], installed: [], loading: false, refresh }}
    >
      {children}
    </ClusterContext.Provider>
  );
}

export function useClusters(): ClusterContextValue {
  const ctx = useContext(ClusterContext);
  if (!ctx)
    throw new Error("useClusters must be used within a ClusterProvider");
  return ctx;
}
