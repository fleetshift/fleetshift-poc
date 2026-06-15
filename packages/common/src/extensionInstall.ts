export interface CoreExtensionMeta {
  navSection: "main" | "bottom";
  learnMoreUrl?: string;
}

export const CORE_EXTENSION_DEFAULTS: Record<string, boolean> = {
  "overview-plugin": true,
  "core-plugin": true,
  "management-plugin": true,
  "signing-plugin": true,
  "gcphcp-plugin": true,
  "kind-plugin": true,
  "configuration-plugin": false,
  "virtualization-plugin": false,
  "security-plugin": false,
  "observability-plugin": false,
  "settings-plugin": true,
};

export const CORE_EXTENSION_META: Record<string, CoreExtensionMeta> = {
  "overview-plugin": { navSection: "main" },
  "core-plugin": { navSection: "main" },
  "management-plugin": { navSection: "main" },
  "signing-plugin": { navSection: "bottom" },
  "gcphcp-plugin": { navSection: "main" },
  "kind-plugin": { navSection: "main" },
  "configuration-plugin": { navSection: "main" },
  "virtualization-plugin": { navSection: "main" },
  "security-plugin": { navSection: "main" },
  "observability-plugin": { navSection: "main" },
  "settings-plugin": { navSection: "bottom" },
};

// --- IndexedDB persistence ---
// TODO: implement together
const EXTENSION_DB_NAME = "ome-extenions";
const EXTENSION_DB_VERSION = 2;

// extension DB stores
const EXTENSIONS_INSTALL_STATE_KEY = "install-state";
const NAV_ORDER_KEY = "nav-order";

function openDb() {
  const omeExtensionsReq = indexedDB.open(
    EXTENSION_DB_NAME,
    EXTENSION_DB_VERSION,
  );
  return new Promise<IDBDatabase>((res, rej) => {
    omeExtensionsReq.onerror = () => {
      // propagate error
      return rej(omeExtensionsReq.error);
    };

    omeExtensionsReq.onupgradeneeded = (event) => {
      const db = omeExtensionsReq.result;
      const oldVersion = (event as IDBVersionChangeEvent).oldVersion;
      if (oldVersion < 1) {
        db.createObjectStore(EXTENSIONS_INSTALL_STATE_KEY);
      }
      if (oldVersion < 2) {
        db.createObjectStore(NAV_ORDER_KEY);
      }
    };

    omeExtensionsReq.onsuccess = () => {
      return res(omeExtensionsReq.result);
    };
  });
}

export class InvalidExtensionStateDBKeyError extends Error {
  constructor(key: IDBValidKey) {
    super(
      `Invalid extension state DB key type: expected string, got: ${typeof key}; key value ${key}`,
    );
    this.name = "InvalidExtensionStateDBKeyError";
  }
}

export class InvalidExtensionStateDBValueError extends Error {
  constructor(key: string, value: unknown) {
    super(
      `Invalid extension state value for key ${key}: expected boolean, got ${typeof value}; actual value: ${value}`,
    );
    this.name = "InvalidExtensionStateDBValueError";
  }
}

export async function getInstallState() {
  try {
    const db = await openDb();
    return new Promise<Record<string, boolean>>((res, rej) => {
      const tx = db.transaction(EXTENSIONS_INSTALL_STATE_KEY, "readonly");
      const store = tx.objectStore(EXTENSIONS_INSTALL_STATE_KEY);
      const cursorReq = store.openCursor();

      const result: Record<string, boolean> = {};
      cursorReq.onsuccess = () => {
        const cursor = cursorReq.result;
        if (cursor) {
          const key = cursor.key;
          const value = cursor.value;
          if (typeof key !== "string") {
            return rej(new InvalidExtensionStateDBKeyError(key));
          }

          if (typeof value !== "boolean") {
            return rej(new InvalidExtensionStateDBValueError(key, value));
          }

          result[key] = value;
          cursor.continue();
        }
      };

      tx.onerror = () => {
        return rej(tx.error);
      };

      tx.oncomplete = () => {
        return res(result);
      };
    });
  } catch (error) {
    // should be some properr logging
    console.error(error);
    throw new Error("Unable to open extension DB");
  }
}

export async function setInstalled(scope: string, installed: boolean) {
  if (typeof scope !== "string") {
    throw new InvalidExtensionStateDBKeyError(scope);
  }

  if (typeof installed !== "boolean") {
    throw new InvalidExtensionStateDBValueError(scope, installed);
  }
  try {
    const db = await openDb();
    return new Promise<void>((res, rej) => {
      const tx = db.transaction(EXTENSIONS_INSTALL_STATE_KEY, "readwrite");
      const store = tx.objectStore(EXTENSIONS_INSTALL_STATE_KEY);

      // value goes frist into DB, then key
      store.put(installed, scope);
      tx.onerror = () => {
        rej(tx.error);
      };

      tx.oncomplete = () => {
        // we have no return value
        res();
      };
    });
  } catch (error) {
    console.error(error);
    throw new Error("Unable to open extension DB");
  }
}

export async function initializeDefaults() {
  try {
    const db = await openDb();
    return new Promise<void>((res, rej) => {
      const tx = db.transaction(EXTENSIONS_INSTALL_STATE_KEY, "readwrite");
      const store = tx.objectStore(EXTENSIONS_INSTALL_STATE_KEY);
      const keysReq = store.getAllKeys();

      keysReq.onsuccess = () => {
        const existing = new Set(keysReq.result);
        for (const [key, value] of Object.entries(CORE_EXTENSION_DEFAULTS)) {
          if (!existing.has(key)) {
            store.put(value, key);
          }
        }
      };

      tx.onerror = () => {
        return rej(tx.error);
      };

      tx.oncomplete = () => {
        return res();
      };
    });
  } catch (error) {
    console.error(error);
    throw new Error("Unable to open extension DB");
  }
}

const NAV_ORDER_ENTRY_KEY = "order";

export async function getNavOrder(): Promise<string[] | null> {
  try {
    const db = await openDb();
    return new Promise<string[] | null>((res, rej) => {
      const tx = db.transaction(NAV_ORDER_KEY, "readonly");
      const store = tx.objectStore(NAV_ORDER_KEY);
      const req = store.get(NAV_ORDER_ENTRY_KEY);

      req.onsuccess = () => {
        const value = req.result;
        if (!Array.isArray(value)) {
          return res(null);
        }
        return res(value as string[]);
      };

      tx.onerror = () => {
        return rej(tx.error);
      };
    });
  } catch (error) {
    console.error(error);
    throw new Error("Unable to open extension DB");
  }
}

export async function setNavOrder(order: string[]): Promise<void> {
  try {
    const db = await openDb();
    return new Promise<void>((res, rej) => {
      const tx = db.transaction(NAV_ORDER_KEY, "readwrite");
      const store = tx.objectStore(NAV_ORDER_KEY);
      store.put(order, NAV_ORDER_ENTRY_KEY);

      tx.onerror = () => {
        rej(tx.error);
      };

      tx.oncomplete = () => {
        res();
      };
    });
  } catch (error) {
    console.error(error);
    throw new Error("Unable to open extension DB");
  }
}

type ExtensionStore = {
  init: typeof initializeDefaults;
  setInstalled: typeof setInstalled;
  getInstallState: typeof getInstallState;
  subscribe: (listener: (state: Record<string, boolean>) => void) => () => void;
  getNavOrder: typeof getNavOrder;
  setNavOrder: typeof setNavOrder;
  subscribeNavOrder: (listener: (order: string[] | null) => void) => () => void;
};

let extensionStore: ExtensionStore;

export function getExtensionStore(): ExtensionStore {
  if (!extensionStore) {
    const subs = new Set<(state: Record<string, boolean>) => void>();
    const navSubs = new Set<(order: string[] | null) => void>();

    function subscribe(cb: (state: Record<string, boolean>) => void) {
      subs.add(cb);
      return () => {
        subs.delete(cb);
      };
    }

    function subscribeNavOrder(cb: (order: string[] | null) => void) {
      navSubs.add(cb);
      return () => {
        navSubs.delete(cb);
      };
    }

    async function notify() {
      const state = await getInstallState();
      for (const cb of subs.values()) {
        cb(state);
      }
    }

    async function notifyNavOrder() {
      const order = await getNavOrder();
      for (const cb of navSubs.values()) {
        cb(order);
      }
    }

    extensionStore = {
      init: async (...args) => {
        await initializeDefaults(...args);
        await notify();
      },
      setInstalled: async (...args) => {
        await setInstalled(...args);
        await notify();
      },
      getInstallState,
      subscribe,
      getNavOrder,
      setNavOrder: async (...args) => {
        await setNavOrder(...args);
        await notifyNavOrder();
      },
      subscribeNavOrder,
    };
  }

  return extensionStore;
}
