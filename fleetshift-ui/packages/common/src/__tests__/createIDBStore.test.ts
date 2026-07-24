import "fake-indexeddb/auto";

import { describe, expect, it, vi } from "vitest";

import { createIDBStore } from "../idb/createIDBStore";

function makeStore<T>(name: string, validate?: (raw: unknown) => T | null) {
  return createIDBStore<T>({
    db: name,
    version: 1,
    store: "items",
    upgrade(db) {
      db.createObjectStore("items");
    },
    validate,
  });
}

describe("createIDBStore", () => {
  it("put + get round-trips a value", async () => {
    const store = makeStore<string>("test-get");
    await store.put("k1", "hello");
    expect(await store.get("k1")).toBe("hello");
  });

  it("get returns null for missing key", async () => {
    const store = makeStore<string>("test-missing");
    expect(await store.get("nope")).toBeNull();
  });

  it("delete removes a key", async () => {
    const store = makeStore<number>("test-delete");
    await store.put("a", 42);
    await store.delete("a");
    expect(await store.get("a")).toBeNull();
  });

  it("getAll returns all entries", async () => {
    const store = makeStore<boolean>("test-getall");
    await store.put("x", true);
    await store.put("y", false);
    expect(await store.getAll()).toEqual({ x: true, y: false });
  });

  it("validate filters out bad reads on get", async () => {
    const raw = makeStore<unknown>("test-validate-get");
    await raw.put("k", "not-a-number");

    const typed = makeStore<number>("test-validate-get", (v) =>
      typeof v === "number" ? v : null,
    );
    expect(await typed.get("k")).toBeNull();
  });

  it("validate filters out bad reads on getAll", async () => {
    const raw = makeStore<unknown>("test-validate-all");
    await raw.put("good", 1);
    await raw.put("bad", "nope");

    const typed = makeStore<number>("test-validate-all", (v) =>
      typeof v === "number" ? v : null,
    );
    expect(await typed.getAll()).toEqual({ good: 1 });
  });

  it("subscribe is called on put", async () => {
    const store = makeStore<string>("test-sub-put");
    const listener = vi.fn();
    store.subscribe(listener);

    await store.put("k", "v");
    expect(listener).toHaveBeenCalledOnce();
  });

  it("subscribe is called on delete", async () => {
    const store = makeStore<string>("test-sub-del");
    await store.put("k", "v");

    const listener = vi.fn();
    store.subscribe(listener);
    await store.delete("k");
    expect(listener).toHaveBeenCalledOnce();
  });

  it("unsubscribe stops notifications", async () => {
    const store = makeStore<string>("test-unsub");
    const listener = vi.fn();
    const unsub = store.subscribe(listener);

    unsub();
    await store.put("k", "v");
    expect(listener).not.toHaveBeenCalled();
  });

  it("put overwrites existing value", async () => {
    const store = makeStore<number>("test-overwrite");
    await store.put("k", 1);
    await store.put("k", 2);
    expect(await store.get("k")).toBe(2);
  });

  it("handles multiple stores sharing same db via upgrade", async () => {
    const store1 = createIDBStore<string>({
      db: "multi-store-db",
      version: 2,
      store: "store-a",
      upgrade(db, oldVersion) {
        if (oldVersion < 1) db.createObjectStore("store-a");
        if (oldVersion < 2) db.createObjectStore("store-b");
      },
    });

    const store2 = createIDBStore<number>({
      db: "multi-store-db",
      version: 2,
      store: "store-b",
      upgrade(db, oldVersion) {
        if (oldVersion < 1) db.createObjectStore("store-a");
        if (oldVersion < 2) db.createObjectStore("store-b");
      },
    });

    await store1.put("name", "alice");
    await store2.put("age", 30);

    expect(await store1.get("name")).toBe("alice");
    expect(await store2.get("age")).toBe(30);
  });
});
