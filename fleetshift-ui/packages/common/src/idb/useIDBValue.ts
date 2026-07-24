/**
 * React hook that reads a single value from a {@link TypedIDBStore} and
 * re-renders when the store is written to.
 */

import { useCallback, useEffect, useMemo, useState } from "react";

import type { TypedIDBStore } from "./createIDBStore.js";

export interface UseIDBValueResult<T> {
  /** The current value, or `null` when missing / not yet loaded. */
  value: T | null;
  /** `true` once the initial read has completed. */
  loaded: boolean;
  /** Persist a new value under the same key. */
  set: (v: T) => Promise<void>;
}

export function useIDBValue<T>(
  store: TypedIDBStore<T>,
  key: string,
): UseIDBValueResult<T> {
  const [value, setValue] = useState<T | null>(null);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setValue(null);
    setLoaded(false);

    function read() {
      store
        .get(key)
        .then((v) => {
          if (!cancelled) {
            setValue(v);
            setLoaded(true);
          }
        })
        .catch((err) => {
          console.error(err);
          if (!cancelled) setLoaded(true);
        });
    }

    // Initial read
    read();

    // Re-read on store changes
    const unsub = store.subscribe(() => {
      if (!cancelled) read();
    });

    return () => {
      cancelled = true;
      unsub();
    };
  }, [store, key]);

  const set = useCallback((v: T) => store.put(key, v), [store, key]);

  const result = useMemo(() => ({ value, loaded, set }), [value, loaded, set]);
  return result;
}

/**
 * React hook that reads all entries from a {@link TypedIDBStore} as a
 * `Record<string, T>` and re-renders on changes.
 */
export interface UseIDBMapResult<T> {
  /** All entries, or empty object before first load. */
  entries: Record<string, T>;
  /** `true` once the initial read has completed. */
  loaded: boolean;
}

export function useIDBMap<T>(store: TypedIDBStore<T>): UseIDBMapResult<T> {
  const [entries, setEntries] = useState<Record<string, T>>({});
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    let cancelled = false;

    function read() {
      store
        .getAll()
        .then((all) => {
          if (!cancelled) {
            setEntries(all);
            setLoaded(true);
          }
        })
        .catch((err) => {
          console.error(err);
          if (!cancelled) setLoaded(true);
        });
    }

    read();

    const unsub = store.subscribe(() => {
      if (!cancelled) read();
    });

    return () => {
      cancelled = true;
      unsub();
    };
  }, [store]);

  const result = useMemo(() => ({ entries, loaded }), [entries, loaded]);
  return result;
}
