import {
  createContext,
  ReactNode,
  useCallback,
  useContext,
  useState,
} from "react";

type Scope = "all" | string;

interface ScopeContextValue {
  scope: Scope;
  setScope: (scope: Scope) => void;
  scopedClusterIds: string[];
  clusterIdsForPlugin: (pluginKey: string) => string[];
}

const ScopeContext = createContext<ScopeContextValue | null>(null);

export function ScopeProvider({ children }: { children: ReactNode }) {
  const [scope, setScope] = useState<Scope>("all");
  const clusterIdsForPlugin = useCallback(() => [] as string[], []);

  return (
    <ScopeContext.Provider
      value={{ scope, setScope, scopedClusterIds: [], clusterIdsForPlugin }}
    >
      {children}
    </ScopeContext.Provider>
  );
}

export function useScope(): ScopeContextValue {
  const ctx = useContext(ScopeContext);
  if (!ctx) throw new Error("useScope must be used within a ScopeProvider");
  return ctx;
}
