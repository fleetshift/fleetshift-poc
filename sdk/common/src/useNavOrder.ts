import { useCallback, useEffect, useMemo, useState } from "react";

import { getExtensionStore } from "./extensionInstall.js";

function useNavOrder() {
  const [loaded, setLoaded] = useState(false);
  const [order, setOrderState] = useState<string[] | null>(null);
  const store = useMemo(getExtensionStore, [getExtensionStore]);

  useEffect(() => {
    let cancelled = false;
    store
      .getNavOrder()
      .then((saved) => {
        if (!cancelled) {
          setOrderState(saved);
          setLoaded(true);
        }
      })
      .catch((err) => {
        console.error(err);
        if (!cancelled) setLoaded(true);
      });
    const unsub = store.subscribeNavOrder((o) => {
      if (!cancelled) setOrderState(o);
    });
    return () => {
      cancelled = true;
      unsub();
    };
  }, [store]);

  const setOrder = useCallback(
    (newOrder: string[]) => store.setNavOrder(newOrder),
    [store],
  );

  return useMemo(
    () => ({ order, loaded, setOrder }),
    [order, loaded, setOrder],
  );
}

export default useNavOrder;
