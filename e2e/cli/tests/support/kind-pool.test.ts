/* eslint-disable playwright/no-standalone-expect -- Vitest test callbacks are not Playwright tests. */
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  isPooledKindClusterId,
  KindClusterPool,
  type KindClusterRequest,
  pooledKindClusterIdPrefix,
} from "./kind-pool";

const READ_ONLY_ANY: KindClusterRequest = {
  access: "read-only",
  state: "any",
};
const READ_ONLY_CLEAN: KindClusterRequest = {
  access: "read-only",
  state: "clean",
};
const MODIFIABLE_ANY: KindClusterRequest = {
  access: "modifiable",
  state: "any",
};

const directories: string[] = [];

async function tempDir(): Promise<string> {
  const directory = await mkdtemp(path.join(os.tmpdir(), "kind-pool-"));
  directories.push(directory);
  return directory;
}

function pool(directory: string, lockTimeoutMs = 5_000): KindClusterPool {
  return new KindClusterPool({
    directory,
    idPrefix: pooledKindClusterIdPrefix("kind-e2e-test-"),
    lockTimeoutMs,
  });
}

afterEach(async () => {
  await Promise.all(
    directories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("pooled Kind cluster IDs", () => {
  it("uses the run-wide pool- prefix and rejects private IDs", () => {
    const prefix = "kind-e2e-abcd-";
    expect(pooledKindClusterIdPrefix(prefix)).toBe("kind-e2e-abcd-pool-");
    expect(isPooledKindClusterId("kind-e2e-abcd-pool-1a2b3c4d", prefix)).toBe(
      true,
    );
    expect(isPooledKindClusterId("kind-e2e-abcd-1a2b3c4d", prefix)).toBe(false);
  });
});

describe("KindClusterPool lifecycle", () => {
  it("reserves new IDs, activates them, and reuses them after a successful release", async () => {
    const clusters = pool(await tempDir());

    const created = await clusters.reserve([READ_ONLY_ANY]);
    expect(created.allocations).toEqual([
      {
        cluster: { id: created.allocations[0]?.cluster.id },
        needsProvisioning: true,
        request: READ_ONLY_ANY,
      },
    ]);
    expect(created.allocations[0]?.cluster.id).toMatch(
      /^kind-e2e-test-pool-[a-f0-9]{8}$/,
    );

    await clusters.activate(created);
    await clusters.release(created, "reusable");

    const reused = await clusters.reserve([READ_ONLY_ANY]);
    expect(reused.allocations).toEqual([
      {
        cluster: created.allocations[0]?.cluster,
        needsProvisioning: false,
        request: READ_ONLY_ANY,
      },
    ]);
    expect(reused.leaseId).not.toBe(created.leaseId);
  });

  it("cancels an unused reservation without publishing new clusters", async () => {
    const clusters = pool(await tempDir());
    const reservation = await clusters.reserve([READ_ONLY_ANY]);
    await clusters.release(reservation, "unused");

    const next = await clusters.reserve([READ_ONLY_ANY]);
    expect(next.allocations[0]?.needsProvisioning).toBe(true);
    expect(next.allocations[0]?.cluster.id).not.toBe(
      reservation.allocations[0]?.cluster.id,
    );
  });

  it("unused release clears existing leases without condition transitions", async () => {
    const clusters = pool(await tempDir());
    const created = await clusters.reserve([READ_ONLY_CLEAN]);
    await clusters.activate(created);
    await clusters.release(created, "reusable");

    const leased = await clusters.reserve([MODIFIABLE_ANY]);
    expect(leased.allocations[0]?.cluster).toEqual(
      created.allocations[0]?.cluster,
    );
    await clusters.release(leased, "unused");

    const stillClean = await clusters.reserve([READ_ONLY_CLEAN]);
    expect(stillClean.allocations[0]?.cluster).toEqual(
      created.allocations[0]?.cluster,
    );
    expect(stillClean.allocations[0]?.needsProvisioning).toBe(false);
  });

  it("releases a successful modifiable lease as modified and evicts discarded records", async () => {
    const clusters = pool(await tempDir());
    const created = await clusters.reserve([MODIFIABLE_ANY]);
    await clusters.activate(created);
    await clusters.release(created, "reusable");

    const modified = await clusters.reserve([READ_ONLY_ANY]);
    expect(modified.allocations[0]?.cluster).toEqual(
      created.allocations[0]?.cluster,
    );
    const missingClean = await clusters.reserve([READ_ONLY_CLEAN]);
    expect(missingClean.allocations[0]?.needsProvisioning).toBe(true);
    await clusters.release(missingClean, "unused");
    await clusters.release(modified, "discarded");

    const replacement = await clusters.reserve([READ_ONLY_ANY]);
    expect(replacement.allocations[0]?.needsProvisioning).toBe(true);
    expect(replacement.allocations[0]?.cluster.id).not.toBe(
      created.allocations[0]?.cluster.id,
    );
  });

  it("ignores empty reservations and does not publish clusters", async () => {
    const clusters = pool(await tempDir());
    const empty = await clusters.reserve([]);
    expect(empty.allocations).toEqual([]);
    await clusters.release(empty, "unused");

    const next = await clusters.reserve([READ_ONLY_ANY]);
    expect(next.allocations[0]?.needsProvisioning).toBe(true);
  });

  it.each([
    { name: "invalid JSON", body: "{not-json" },
    { name: "non-array records", body: '{"records":null}' },
    {
      name: "empty id",
      body: '{"records":[{"id":"","condition":"clean"}]}',
    },
    {
      name: "unknown condition",
      body: '{"records":[{"id":"c","condition":"dirty"}]}',
    },
    {
      name: "non-string leaseId",
      body: '{"records":[{"id":"c","condition":"clean","leaseId":1}]}',
    },
  ])("fails clearly when pool state is malformed ($name)", async ({ body }) => {
    const directory = await tempDir();
    await mkdir(directory, { mode: 0o700, recursive: true });
    await writeFile(path.join(directory, "state.json"), body, {
      mode: 0o600,
    });
    await expect(pool(directory).reserve([READ_ONLY_ANY])).rejects.toThrow(
      /malformed/,
    );
  });

  it("releases the allocation lock after a failed action", async () => {
    const directory = await tempDir();
    await mkdir(directory, { mode: 0o700, recursive: true });
    await writeFile(path.join(directory, "state.json"), "{", { mode: 0o600 });
    await expect(pool(directory).reserve([READ_ONLY_ANY])).rejects.toThrow(
      /malformed/,
    );
    await writeFile(
      path.join(directory, "state.json"),
      JSON.stringify({ records: [] }),
      { mode: 0o600 },
    );
    const reservation = await pool(directory).reserve([READ_ONLY_ANY]);
    expect(reservation.allocations[0]?.needsProvisioning).toBe(true);
  });

  it("fails clearly when the allocation lock cannot be acquired", async () => {
    const directory = await tempDir();
    await mkdir(path.join(directory, "allocation-lock"), {
      mode: 0o700,
      recursive: true,
    });
    await expect(pool(directory, 30).reserve([READ_ONLY_ANY])).rejects.toThrow(
      /allocation lock/,
    );
  });
});

describe("KindClusterPool concurrency", () => {
  it("leases exclusive ownership and keeps a multi-cluster reservation atomic", async () => {
    const directory = await tempDir();
    const first = pool(directory);
    const second = pool(directory);
    const seed = await first.reserve([READ_ONLY_ANY, READ_ONLY_ANY]);
    await first.activate(seed);
    await first.release(seed, "reusable");

    const [one, two] = await Promise.all([
      first.reserve([READ_ONLY_ANY, READ_ONLY_ANY]),
      second.reserve([READ_ONLY_ANY, READ_ONLY_ANY]),
    ]);
    const seeded = new Set(seed.allocations.map(({ cluster }) => cluster.id));
    const oneExisting = one.allocations.filter(
      (allocation) => !allocation.needsProvisioning,
    );
    const twoExisting = two.allocations.filter(
      (allocation) => !allocation.needsProvisioning,
    );
    expect([oneExisting.length, twoExisting.length].sort()).toEqual([0, 2]);
    const winner = oneExisting.length === 2 ? one : two;
    expect(winner.allocations.map(({ cluster }) => cluster.id).sort()).toEqual(
      [...seeded].sort(),
    );

    const created = [...one.allocations, ...two.allocations]
      .filter((allocation) => allocation.needsProvisioning)
      .map(({ cluster }) => cluster.id);
    expect(new Set(created).size).toBe(created.length);
    expect(created.every((id) => !seeded.has(id))).toBe(true);
  });

  it("gives concurrent empty-pool reservations distinct new IDs", async () => {
    const directory = await tempDir();
    const [one, two] = await Promise.all([
      pool(directory).reserve([READ_ONLY_ANY]),
      pool(directory).reserve([READ_ONLY_ANY]),
    ]);
    expect(one.allocations[0]?.cluster.id).not.toBe(
      two.allocations[0]?.cluster.id,
    );
    expect(one.allocations[0]?.needsProvisioning).toBe(true);
    expect(two.allocations[0]?.needsProvisioning).toBe(true);
  });
});
