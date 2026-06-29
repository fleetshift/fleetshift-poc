import { useCallback, useEffect, useMemo, useState } from "react";

import { getExtensionStore } from "./extensionInstall.js";
import type { NavLayoutOverride } from "./navLayout.js";

export interface UseNavLayoutResult {
  /** New-format override from IndexedDB, or null if not set. */
  override: NavLayoutOverride | null;
  /** Legacy flat ordering from IndexedDB, or null if not set / new format exists. */
  legacyOrder: string[] | null;
  /** True once both stores have been read. */
  loaded: boolean;
  /** Persist a new override to IndexedDB. */
  setOverride: (override: NavLayoutOverride) => Promise<void>;
  /** Remove the override from IndexedDB, reverting to backend layout. */
  clearOverride: () => Promise<void>;
}

/**
 * Read the user's nav layout override from IndexedDB.
 *
 * Checks the new `nav-layout` store first. If it has a `NavLayoutOverride`,
 * that wins. Otherwise falls back to the legacy `nav-order` store (flat
 * `string[]`) so existing users keep their ordering until they save from
 * the new editor.
 */
function useNavLayout(): UseNavLayoutResult {
  const [loaded, setLoaded] = useState(false);
  const [override, setOverrideState] = useState<NavLayoutOverride | null>(null);
  const [legacyOrder, setLegacyOrder] = useState<string[] | null>(null);
  const store = useMemo(getExtensionStore, [getExtensionStore]);

  useEffect(() => {
    let cancelled = false;

    // Read both stores in parallel
    Promise.all([store.getNavLayout(), store.getNavOrder()])
      .then(([layout, order]) => {
        if (cancelled) return;
        if (layout) {
          // New format takes precedence
          setOverrideState(layout);
          setLegacyOrder(null);
        } else {
          // Fall back to legacy flat ordering
          setOverrideState(null);
          setLegacyOrder(order);
        }
        setLoaded(true);
      })
      .catch((err) => {
        console.error(err);
        if (!cancelled) setLoaded(true);
      });

    // Subscribe to changes in both stores
    const unsubLayout = store.subscribeNavLayout((layout) => {
      if (cancelled) return;
      if (layout) {
        setOverrideState(layout);
        setLegacyOrder(null);
      }
    });

    const unsubOrder = store.subscribeNavOrder((order) => {
      if (!cancelled) setLegacyOrder(order);
    });

    return () => {
      cancelled = true;
      unsubLayout();
      unsubOrder();
    };
  }, [store]);

  const setOverride = useCallback(
    (newOverride: NavLayoutOverride) => store.setNavLayout(newOverride),
    [store],
  );

  const clearOverride = useCallback(() => store.clearNavLayout(), [store]);

  return useMemo(
    () => ({ override, legacyOrder, loaded, setOverride, clearOverride }),
    [override, legacyOrder, loaded, setOverride, clearOverride],
  );
}

export default useNavLayout;
