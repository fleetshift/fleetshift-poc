import { useCallback, useEffect, useMemo, useState } from "react";

import {
  CORE_EXTENSION_DEFAULTS,
  getExtensionStore,
} from "./extensionInstall.js";

function useExtensionInstall() {
  const [loaded, setLoaded] = useState(false);
  const [state, subs] = useState<Record<string, boolean>>({});
  const store = useMemo(getExtensionStore, [getExtensionStore]);
  const [error, setError] = useState<unknown | undefined>(undefined);

  const initState = useCallback(async () => {
    try {
      await store.init();
      const initState = await store.getInstallState();
      subs(initState);
      setLoaded(true);
    } catch (error: unknown) {
      console.error(error);
      setError(error);
      setLoaded(true);
    }
  }, [store, subs, setLoaded, setError]);

  useEffect(() => {
    initState();
    const unsub = store.subscribe(subs);
    return () => {
      unsub();
    };
  }, [initState, store]);

  const isInstalled = useCallback(
    (scope: string) => {
      if (scope in state) return !!state[scope];
      if (scope in CORE_EXTENSION_DEFAULTS)
        return CORE_EXTENSION_DEFAULTS[scope];
      return true;
    },
    [state],
  );

  const install = useCallback(
    (scope: string) => store.setInstalled(scope, true),
    [store],
  );
  const uninstall = useCallback(
    (scope: string) => store.setInstalled(scope, false),
    [store],
  );

  const response = useMemo(
    () => ({
      state,
      loaded,
      error,
      isInstalled,
      install,
      uninstall,
    }),
    [state, loaded, error, isInstalled, install, uninstall],
  );
  return response;
}

export default useExtensionInstall;
