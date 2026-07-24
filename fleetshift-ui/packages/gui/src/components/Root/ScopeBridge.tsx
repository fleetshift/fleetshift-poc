import { useEffect } from "react";

import { useScope } from "../../contexts/ScopeContext";

export const scopeRef = { current: "all" as string };
export const scopeListenersRef = { current: new Set<() => void>() };

const ScopeBridge = () => {
  const { scope } = useScope();
  useEffect(() => {
    scopeRef.current = scope;
    scopeListenersRef.current.forEach((fn) => fn());
  }, [scope]);
  return null;
};

export default ScopeBridge;
