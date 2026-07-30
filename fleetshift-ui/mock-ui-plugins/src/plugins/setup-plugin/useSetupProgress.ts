import { useCallback, useEffect, useMemo, useState } from "react";

import { getSetupProgressStore } from "./setupProgress";

export function useSetupProgress() {
  const [loaded, setLoaded] = useState(false);
  const [progress, setProgress] = useState<Record<string, boolean>>({});
  const store = useMemo(getSetupProgressStore, []);

  useEffect(() => {
    let cancelled = false;
    store
      .getProgress()
      .then((state) => {
        if (!cancelled) {
          setProgress(state);
          setLoaded(true);
        }
      })
      .catch((err) => {
        console.error(err);
        if (!cancelled) setLoaded(true);
      });
    const unsub = store.subscribe((state) => {
      if (!cancelled) setProgress(state);
    });
    return () => {
      cancelled = true;
      unsub();
    };
  }, [store]);

  const setStepComplete = useCallback(
    (stepId: string, complete: boolean) =>
      store.setStepComplete(stepId, complete),
    [store],
  );

  return useMemo(
    () => ({ progress, loaded, setStepComplete }),
    [progress, loaded, setStepComplete],
  );
}
