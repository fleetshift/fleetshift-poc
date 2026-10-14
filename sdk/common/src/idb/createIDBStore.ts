/**
 * Lightweight factory for typed IndexedDB stores.
 *
 * Eliminates repeated open/transaction/error boilerplate. Covers two
 * patterns used throughout the codebase:
 *   - **Single-value**: get/put one typed value by a fixed key.
 *   - **Map**: iterate all entries as `Record<string, T>`.
 *
 * Each store instance caches the DB connection and provides a simple
 * pub/sub mechanism so React hooks can subscribe to changes.
 */

export interface IDBStoreConfig<T> {
  /** Database name. */
  db: string;
  /** Database version — drives `onupgradeneeded`. */
  version: number;
  /** Object store name inside the database. */
  store: string;
  /** Called during `onupgradeneeded` to create/migrate object stores. */
  upgrade: (db: IDBDatabase, oldVersion: number) => void;
  /** Optional validator applied on reads. Return `null` to discard bad data. */
  validate?: (raw: unknown) => T | null;
}

export interface TypedIDBStore<T> {
  /** Read a single value by key. Returns `null` when missing or invalid. */
  get(key: string): Promise<T | null>;
  /** Write a value under the given key. Notifies subscribers. */
  put(key: string, value: T): Promise<void>;
  /** Delete a key. Notifies subscribers. */
  delete(key: string): Promise<void>;
  /** Read every entry as a key/value map. */
  getAll(): Promise<Record<string, T>>;
  /** Subscribe to write/delete notifications. Returns an unsubscribe fn. */
  subscribe(listener: () => void): () => void;
}

export function createIDBStore<T>(config: IDBStoreConfig<T>): TypedIDBStore<T> {
  let dbPromise: Promise<IDBDatabase> | null = null;
  const subs = new Set<() => void>();

  function open(): Promise<IDBDatabase> {
    if (!dbPromise) {
      dbPromise = new Promise<IDBDatabase>((resolve, reject) => {
        const req = indexedDB.open(config.db, config.version);
        req.onupgradeneeded = (event) => {
          config.upgrade(req.result, event.oldVersion);
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => {
          dbPromise = null;
          reject(req.error);
        };
      });
    }
    return dbPromise;
  }

  function notify() {
    for (const cb of subs) {
      cb();
    }
  }

  async function get(key: string): Promise<T | null> {
    const db = await open();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(config.store, "readonly");
      const req = tx.objectStore(config.store).get(key);
      req.onsuccess = () => {
        const raw = req.result;
        if (raw === undefined) return resolve(null);
        if (config.validate) return resolve(config.validate(raw));
        return resolve(raw as T);
      };
      tx.onerror = () => reject(tx.error);
    });
  }

  async function put(key: string, value: T): Promise<void> {
    const db = await open();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(config.store, "readwrite");
      tx.objectStore(config.store).put(value, key);
      tx.oncomplete = () => {
        notify();
        resolve();
      };
      tx.onerror = () => reject(tx.error);
    });
  }

  async function del(key: string): Promise<void> {
    const db = await open();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(config.store, "readwrite");
      tx.objectStore(config.store).delete(key);
      tx.oncomplete = () => {
        notify();
        resolve();
      };
      tx.onerror = () => reject(tx.error);
    });
  }

  async function getAll(): Promise<Record<string, T>> {
    const db = await open();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(config.store, "readonly");
      const cursor = tx.objectStore(config.store).openCursor();
      const result: Record<string, T> = {};

      cursor.onsuccess = () => {
        const c = cursor.result;
        if (c) {
          if (typeof c.key === "string") {
            if (config.validate) {
              const validated = config.validate(c.value);
              if (validated !== null) {
                result[c.key] = validated;
              }
            } else {
              result[c.key] = c.value as T;
            }
          }
          c.continue();
        }
      };
      tx.oncomplete = () => resolve(result);
      tx.onerror = () => reject(tx.error);
    });
  }

  function subscribe(listener: () => void): () => void {
    subs.add(listener);
    return () => {
      subs.delete(listener);
    };
  }

  return { get, put, delete: del, getAll, subscribe };
}
