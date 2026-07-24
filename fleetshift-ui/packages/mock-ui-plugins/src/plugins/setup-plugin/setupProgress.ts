import { createIDBStore } from "@fleetshift/common";

const setupStore = createIDBStore<boolean>({
  db: "ome-setup-progress",
  version: 1,
  store: "steps",
  upgrade(db) {
    if (!db.objectStoreNames.contains("steps")) {
      db.createObjectStore("steps");
    }
  },
  validate: (raw) => (typeof raw === "boolean" ? raw : null),
});

export type SetupProgressStore = {
  getProgress: () => Promise<Record<string, boolean>>;
  setStepComplete: (stepId: string, complete: boolean) => Promise<void>;
  subscribe: (listener: (state: Record<string, boolean>) => void) => () => void;
};

let store: SetupProgressStore | null = null;

export function getSetupProgressStore(): SetupProgressStore {
  if (!store) {
    const subs = new Set<(state: Record<string, boolean>) => void>();

    async function notify() {
      const state = await setupStore.getAll();
      for (const cb of subs) {
        cb(state);
      }
    }

    store = {
      getProgress: () => setupStore.getAll(),
      setStepComplete: async (stepId, complete) => {
        await setupStore.put(stepId, complete);
        await notify();
      },
      subscribe(cb) {
        subs.add(cb);
        return () => {
          subs.delete(cb);
        };
      },
    };
  }
  return store;
}
