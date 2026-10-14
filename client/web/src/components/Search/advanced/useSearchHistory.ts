import { createIDBStore, useIDBMap } from "@fleetshift/common";
import { useCallback, useMemo } from "react";

import type { HistoryEntry } from "./types";

const MAX_ENTRIES = 50;

const historyStore = createIDBStore<HistoryEntry>({
  db: "fleetshift-search-history",
  version: 1,
  store: "history",
  upgrade: (db) => {
    if (!db.objectStoreNames.contains("history")) {
      db.createObjectStore("history");
    }
  },
});

export function useSearchHistory() {
  const { entries: raw, loaded } = useIDBMap(historyStore);

  const entries = useMemo(() => {
    return Object.values(raw).sort((a, b) => b.timestamp - a.timestamp);
  }, [raw]);

  const save = useCallback(
    async (expression: string) => {
      const trimmed = expression.trim();
      if (!trimmed) return;

      const existing = raw[trimmed];
      const entry: HistoryEntry = {
        expression: trimmed,
        timestamp: Date.now(),
        favorite: existing?.favorite ?? false,
      };
      await historyStore.put(trimmed, entry);

      const sorted = Object.values(raw)
        .concat(existing ? [] : [entry])
        .sort((a, b) => b.timestamp - a.timestamp);

      const nonFavorites = sorted.filter((e) => !e.favorite);
      if (nonFavorites.length > MAX_ENTRIES) {
        const toRemove = nonFavorites.slice(MAX_ENTRIES);
        for (const e of toRemove) {
          await historyStore.delete(e.expression);
        }
      }
    },
    [raw],
  );

  const toggleFavorite = useCallback(
    async (expression: string) => {
      const existing = raw[expression];
      if (!existing) return;
      await historyStore.put(expression, {
        ...existing,
        favorite: !existing.favorite,
      });
    },
    [raw],
  );

  const remove = useCallback(async (expression: string) => {
    await historyStore.delete(expression);
  }, []);

  return { entries, loaded, save, toggleFavorite, remove };
}
