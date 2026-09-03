import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rmdir, writeFile } from "node:fs/promises";
import path from "node:path";

import { uniqueKindClusterId } from "../../../shared/kind-cluster-id";
import {
  isDefaultKindClusterSpec,
  type KindClusterCreateSpec,
  kindClusterSpecKey,
  parseKindClusterCreateSpec,
} from "./kind-spec";

export type KindClusterAccess = "read-only" | "modifiable";
export type KindClusterStateRequirement = "clean" | "any";
export type KindClusterCondition = "clean" | "modified";
export type KindClusterReleaseOutcome = "unused" | "reusable" | "discarded";

export interface KindClusterRequest {
  readonly access: KindClusterAccess;
  readonly state: KindClusterStateRequirement;
  /**
   * When omitted, any already-provisioned spec may be leased; a newly
   * created cluster uses the default empty spec. When present, only a
   * cluster created with this spec is a match (`{}` pins default).
   */
  readonly spec?: KindClusterCreateSpec;
}

export interface KindCluster {
  readonly id: string;
}

export interface KindClusterReservation {
  readonly leaseId: string;
  readonly allocations: readonly {
    readonly cluster: KindCluster;
    readonly request: KindClusterRequest;
    readonly needsProvisioning: boolean;
  }[];
}

export interface AvailableKindCluster {
  readonly id: string;
  readonly condition: KindClusterCondition;
  readonly spec: KindClusterCreateSpec;
}

interface PoolRecord {
  id: string;
  condition: KindClusterCondition;
  leaseId?: string;
  spec: KindClusterCreateSpec;
}

interface PoolState {
  records: PoolRecord[];
}

const STATE_FILE = "state.json";
const LOCK_DIRECTORY = "allocation-lock";
const DEFAULT_LOCK_TIMEOUT_MS = 5_000;
const LOCK_RETRY_MS = 10;

export function pooledKindClusterIdPrefix(kindIdPrefix: string): string {
  return `${kindIdPrefix}pool-`;
}

export function isPooledKindClusterId(
  id: string,
  kindIdPrefix: string,
): boolean {
  return id.startsWith(pooledKindClusterIdPrefix(kindIdPrefix));
}

export function nextKindClusterCondition(
  current: KindClusterCondition,
  request: KindClusterRequest,
): KindClusterCondition {
  return request.access === "modifiable" ? "modified" : current;
}

/**
 * Assigns available clusters to requests without reordering the request
 * list. Specific-spec slots are filled before omitted-spec slots so a mixed
 * reservation cannot spend the only matching topology on an unconstrained
 * request. Omitted spec matches any topology and prefers non-default
 * clusters when condition is equal.
 */
export function matchKindClusterRequests(
  available: readonly AvailableKindCluster[],
  requests: readonly KindClusterRequest[],
): readonly (string | undefined)[] {
  const remaining = [...available];
  const assigned: (string | undefined)[] = Array.from(
    { length: requests.length },
    () => undefined,
  );

  const take = (
    predicate: (cluster: AvailableKindCluster) => boolean,
  ): string | undefined => {
    const index = remaining.findIndex(predicate);
    if (index === -1) return undefined;
    const [cluster] = remaining.splice(index, 1);
    return cluster?.id;
  };

  const takePinned = (
    request: KindClusterRequest,
    condition: KindClusterCondition,
  ): string | undefined => {
    if (request.spec === undefined) return undefined;
    const key = kindClusterSpecKey(request.spec);
    return take(
      (cluster) =>
        cluster.condition === condition &&
        kindClusterSpecKey(cluster.spec) === key,
    );
  };

  const takeUnconstrained = (
    condition: KindClusterCondition,
  ): string | undefined =>
    take(
      (cluster) =>
        cluster.condition === condition &&
        !isDefaultKindClusterSpec(cluster.spec),
    ) ??
    take(
      (cluster) =>
        cluster.condition === condition &&
        isDefaultKindClusterSpec(cluster.spec),
    );

  for (const [index, request] of requests.entries()) {
    if (request.spec !== undefined && request.state === "clean") {
      assigned[index] = takePinned(request, "clean");
    }
  }
  for (const [index, request] of requests.entries()) {
    if (request.spec !== undefined && request.state === "any") {
      assigned[index] =
        takePinned(request, "modified") ?? takePinned(request, "clean");
    }
  }
  for (const [index, request] of requests.entries()) {
    if (request.spec === undefined && request.state === "clean") {
      assigned[index] = takeUnconstrained("clean");
    }
  }
  for (const [index, request] of requests.entries()) {
    if (request.spec === undefined && request.state === "any") {
      assigned[index] =
        takeUnconstrained("modified") ?? takeUnconstrained("clean");
    }
  }
  return assigned;
}

export class KindClusterPool {
  readonly #directory: string;
  readonly #idPrefix: string;
  readonly #lockTimeoutMs: number;

  constructor(options: {
    directory: string;
    idPrefix: string;
    lockTimeoutMs?: number;
  }) {
    this.#directory = options.directory;
    this.#idPrefix = options.idPrefix;
    this.#lockTimeoutMs = options.lockTimeoutMs ?? DEFAULT_LOCK_TIMEOUT_MS;
  }

  async reserve(
    requests: readonly KindClusterRequest[],
  ): Promise<KindClusterReservation> {
    const leaseId = randomUUID();
    if (requests.length === 0) {
      return { leaseId, allocations: [] };
    }
    return this.#withLock(async () => {
      const state = await this.#load();
      const available = state.records
        .filter((record) => record.leaseId == null)
        .map((record): AvailableKindCluster => ({
          condition: record.condition,
          id: record.id,
          spec: record.spec,
        }));
      const assigned = matchKindClusterRequests(available, requests);
      const generated = new Set<string>();
      const allocations = requests.map((request, index) => {
        const existingId = assigned[index];
        if (existingId !== undefined) {
          const record = state.records.find((item) => item.id === existingId);
          if (!record) {
            throw new Error(`kind pool matched unknown cluster ${existingId}`);
          }
          record.leaseId = leaseId;
          return {
            cluster: { id: existingId },
            needsProvisioning: false,
            request,
          };
        }
        const id = this.#uniqueId(state, generated);
        generated.add(id);
        return {
          cluster: { id },
          needsProvisioning: true,
          request,
        };
      });
      await this.#save(state);
      return { leaseId, allocations };
    });
  }

  async activate(
    reservation: KindClusterReservation,
  ): Promise<readonly KindCluster[]> {
    const created = reservation.allocations.filter(
      (allocation) => allocation.needsProvisioning,
    );
    if (created.length > 0) {
      await this.#withLock(async () => {
        const state = await this.#load();
        for (const allocation of created) {
          if (
            state.records.some((record) => record.id === allocation.cluster.id)
          ) {
            throw new Error(
              `kind pool already has cluster ${allocation.cluster.id}`,
            );
          }
          state.records.push({
            condition: "clean",
            id: allocation.cluster.id,
            leaseId: reservation.leaseId,
            spec: allocation.request.spec ?? {},
          });
        }
        await this.#save(state);
      });
    }
    return reservation.allocations.map(({ cluster }) => cluster);
  }

  async release(
    reservation: KindClusterReservation,
    outcome: KindClusterReleaseOutcome,
  ): Promise<void> {
    if (reservation.allocations.length === 0) return;
    await this.#withLock(async () => {
      const state = await this.#load();
      if (outcome === "discarded") {
        state.records = state.records.filter(
          (record) => record.leaseId !== reservation.leaseId,
        );
      } else {
        const requests = new Map(
          reservation.allocations.map((allocation) => [
            allocation.cluster.id,
            allocation.request,
          ]),
        );
        for (const record of state.records) {
          if (record.leaseId !== reservation.leaseId) continue;
          if (outcome === "reusable") {
            const request = requests.get(record.id);
            if (request) {
              record.condition = nextKindClusterCondition(
                record.condition,
                request,
              );
            }
          }
          delete record.leaseId;
        }
      }
      await this.#save(state);
    });
  }

  #uniqueId(state: PoolState, generated: ReadonlySet<string>): string {
    const taken = new Set([
      ...state.records.map((record) => record.id),
      ...generated,
    ]);
    for (;;) {
      const id = uniqueKindClusterId(this.#idPrefix);
      if (!taken.has(id)) return id;
    }
  }

  async #withLock<T>(action: () => Promise<T>): Promise<T> {
    await mkdir(this.#directory, { mode: 0o700, recursive: true });
    const lockDir = path.join(this.#directory, LOCK_DIRECTORY);
    const deadline = Date.now() + this.#lockTimeoutMs;
    for (;;) {
      try {
        await mkdir(lockDir);
        break;
      } catch (error) {
        if (!isAlreadyExists(error) || Date.now() >= deadline) {
          if (isAlreadyExists(error)) {
            throw new Error(
              "kind pool allocation lock was not acquired within the timeout",
            );
          }
          throw error;
        }
        await delay(LOCK_RETRY_MS);
      }
    }
    let actionError: unknown;
    try {
      return await action();
    } catch (error) {
      actionError = error;
      throw error;
    } finally {
      try {
        await rmdir(lockDir);
      } catch (releaseError) {
        if (actionError === undefined && !isNotFound(releaseError)) {
          throw releaseError;
        }
      }
    }
  }

  async #load(): Promise<PoolState> {
    let raw: string;
    try {
      raw = await readFile(path.join(this.#directory, STATE_FILE), "utf8");
    } catch (error) {
      if (isNotFound(error)) return { records: [] };
      throw error;
    }
    return parsePoolState(raw);
  }

  async #save(state: PoolState): Promise<void> {
    const file = path.join(this.#directory, STATE_FILE);
    const temp = path.join(
      this.#directory,
      `${STATE_FILE}.${randomUUID()}.tmp`,
    );
    await writeFile(temp, JSON.stringify(state), { mode: 0o600 });
    await rename(temp, file);
  }
}

function parsePoolState(raw: string): PoolState {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    throw new Error("kind pool state is malformed");
  }
  if (
    typeof value !== "object" ||
    value === null ||
    !("records" in value) ||
    !Array.isArray(value.records)
  ) {
    throw new Error("kind pool state is malformed");
  }
  return { records: value.records.map(parsePoolRecord) };
}

function parsePoolRecord(value: unknown): PoolRecord {
  if (typeof value !== "object" || value === null) {
    throw new Error("kind pool state is malformed");
  }
  const record = value as {
    condition?: unknown;
    id?: unknown;
    leaseId?: unknown;
    spec?: unknown;
  };
  if (typeof record.id !== "string" || record.id.trim() === "") {
    throw new Error("kind pool state is malformed");
  }
  if (record.condition !== "clean" && record.condition !== "modified") {
    throw new Error("kind pool state is malformed");
  }
  if (record.leaseId != null && typeof record.leaseId !== "string") {
    throw new Error("kind pool state is malformed");
  }
  let spec: KindClusterCreateSpec = {};
  if (record.spec !== undefined) {
    try {
      spec = parseKindClusterCreateSpec(record.spec);
    } catch {
      throw new Error("kind pool state is malformed");
    }
  }
  const parsed: PoolRecord = {
    condition: record.condition,
    id: record.id,
    spec,
  };
  if (typeof record.leaseId === "string") parsed.leaseId = record.leaseId;
  return parsed;
}

function isAlreadyExists(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "EEXIST"
  );
}

function isNotFound(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "ENOENT"
  );
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
