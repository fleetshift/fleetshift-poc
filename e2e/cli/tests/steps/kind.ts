import { randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import { expect } from "@playwright/test";

import { uniqueKindClusterIdFromEnv } from "../../../shared/kind-cluster-id";
import { type CleanupStack } from "../support/cleanup";
import { isNotFound } from "../support/command";
import { type FleetctlClient } from "../support/fleetctl";
import { parseJSON } from "../support/json";
import {
  hostKindClusterName,
  kindAPIRequest,
  kindNodeIDs,
  kubectlOnKind,
} from "../support/kind-host";
import { isPooledKindClusterId } from "../support/kind-pool";
import { type Sandbox } from "../support/sandbox";
import { CONFIG_MAP_DATA, CONFIG_MAP_NAME } from "./deployments";

export const KIND_CLUSTER_TYPE = "kind.fleetshift.v1/clusters";
const CLUSTER_WAIT_TIMEOUT_MS = 10 * 60_000;
const OBJECT_WAIT_TIMEOUT_MS = 2 * 60_000;
const OIDC_WAIT_TIMEOUT_MS = 2 * 60_000;
const POLL_INTERVAL_MS = 2_000;
const NAMESPACE_PATH = "/api/v1/namespaces";

interface ClusterView {
  conditions: Record<string, { status?: string }>;
  name: string;
  pauseReason: string;
  state: string;
}

export function clusterResourceName(id: string): string {
  return id.startsWith("clusters/") ? id : `clusters/${id}`;
}

function clusterView(
  value: Partial<ClusterView> | null | undefined,
): ClusterView {
  return {
    conditions: value?.conditions ?? {},
    name: value?.name ?? "",
    pauseReason: value?.pauseReason ?? "",
    state: value?.state ?? "",
  };
}

export function parseCluster(raw: string): ClusterView {
  return clusterView(parseJSON(raw));
}

export function parseClusterList(raw: string): ClusterView[] {
  const value = parseJSON<unknown>(raw);
  if (!Array.isArray(value)) throw new Error("invalid JSON: expected array");
  return value.map((item) => clusterView(item as Partial<ClusterView>));
}

export function parseMetadataName(raw: string): string {
  const value = parseJSON<{ metadata?: { name?: string } } | null>(raw);
  return value?.metadata?.name ?? "";
}

export function parseConfigMapData(raw: string): Record<string, string> {
  const value = parseJSON<{ data?: Record<string, string> } | null>(raw);
  return value?.data ?? {};
}

export function uniqueKindClusterId(): string {
  return uniqueKindClusterIdFromEnv();
}

export class KindSteps {
  readonly #cleanup?: CleanupStack;
  readonly #client: FleetctlClient;
  readonly #sandbox: Sandbox;

  constructor(
    client: FleetctlClient,
    sandbox: Sandbox,
    cleanup?: CleanupStack,
  ) {
    this.#client = client;
    this.#sandbox = sandbox;
    this.#cleanup = cleanup;
  }

  async create(id: string): Promise<void> {
    const dir = path.join(this.#sandbox.workDir, `kind-spec-${randomUUID()}`);
    await mkdir(dir, { mode: 0o700 });
    const specFile = path.join(dir, "spec.json");
    await writeFile(specFile, JSON.stringify({ name: id }), { mode: 0o600 });
    const result = await this.#client.succeed([
      "resource",
      "create",
      KIND_CLUSTER_TYPE,
      "--id",
      id,
      "--spec-file",
      specFile,
    ]);
    expect(parseCluster(result.stdout).name).toBe(clusterResourceName(id));
    this.#cleanup?.add(() => this.#deleteBestEffort(id));
  }

  async waitUntilReady(id: string): Promise<void> {
    await expect
      .poll(
        async () => {
          const result = await this.#client.succeed([
            "resource",
            "get",
            KIND_CLUSTER_TYPE,
            id,
          ]);
          const cluster = parseCluster(result.stdout);
          expect(cluster.name).toBe(clusterResourceName(id));
          if (cluster.state === "FAILED" || cluster.pauseReason.trim()) {
            throw new Error(
              `cluster ${cluster.name} ${cluster.state}${cluster.pauseReason ? `: ${cluster.pauseReason}` : ""}`,
            );
          }
          return {
            pauseReason: cluster.pauseReason,
            ready: cluster.conditions["Ready"]?.status ?? "",
            state: cluster.state,
          };
        },
        { intervals: [POLL_INTERVAL_MS], timeout: CLUSTER_WAIT_TIMEOUT_MS },
      )
      .toEqual({ pauseReason: "", ready: "True", state: "ACTIVE" });
  }

  async #kindAPI(
    id: string,
    method: string,
    apiPath: string,
    options?: { body?: string; configDir?: string },
  ): Promise<{ status: number }> {
    return kindAPIRequest(
      hostKindClusterName(id),
      await this.#client.accessToken(options?.configDir),
      method,
      apiPath,
      options?.body,
    );
  }

  async waitUntilAPIAcceptsToken(id: string): Promise<void> {
    await expect
      .poll(
        async () => {
          const response = await this.#kindAPI(id, "GET", NAMESPACE_PATH);
          return response.status;
        },
        { intervals: [POLL_INTERVAL_MS], timeout: OIDC_WAIT_TIMEOUT_MS },
      )
      .toBe(200);
  }

  async createNamespaceViaOIDC(id: string, namespace: string): Promise<void> {
    await expect
      .poll(
        async () => {
          const response = await this.#kindAPI(id, "POST", NAMESPACE_PATH, {
            body: JSON.stringify({
              apiVersion: "v1",
              kind: "Namespace",
              metadata: { name: namespace },
            }),
          });
          return [201, 409].includes(response.status);
        },
        { intervals: [POLL_INTERVAL_MS], timeout: OIDC_WAIT_TIMEOUT_MS },
      )
      .toBe(true);
  }

  async waitUntilNamespaceExistsViaOIDC(
    id: string,
    namespace: string,
  ): Promise<void> {
    await expect
      .poll(
        async () => {
          const response = await this.#kindAPI(
            id,
            "GET",
            `${NAMESPACE_PATH}/${namespace}`,
          );
          return response.status;
        },
        { intervals: [POLL_INTERVAL_MS], timeout: OIDC_WAIT_TIMEOUT_MS },
      )
      .toBe(200);
  }

  async waitUntilForbidden(id: string, configDir: string): Promise<void> {
    await expect
      .poll(
        async () => {
          const response = await this.#kindAPI(id, "GET", NAMESPACE_PATH, {
            configDir,
          });
          if (response.status !== 403) {
            throw new Error(`Kind API status ${response.status}, want 403`);
          }
          return response.status;
        },
        { intervals: [POLL_INTERVAL_MS], timeout: OIDC_WAIT_TIMEOUT_MS },
      )
      .toBe(403);
  }

  async waitUntilNamespaceExists(id: string, namespace: string): Promise<void> {
    await expect
      .poll(
        async () => {
          const result = await kubectlOnKind(hostKindClusterName(id), [
            "get",
            "namespace",
            namespace,
            "-o",
            "json",
          ]);
          if (result.exitCode !== 0) return { cluster: id, name: "" };
          return { cluster: id, name: parseMetadataName(result.stdout) };
        },
        {
          intervals: [POLL_INTERVAL_MS],
          message: `namespace ${namespace} on ${id}`,
          timeout: OBJECT_WAIT_TIMEOUT_MS,
        },
      )
      .toEqual({ cluster: id, name: namespace });
  }

  async waitUntilConfigMapExists(id: string, namespace: string): Promise<void> {
    await expect
      .poll(
        async () => {
          const result = await kubectlOnKind(hostKindClusterName(id), [
            "get",
            "configmap",
            CONFIG_MAP_NAME,
            "-n",
            namespace,
            "-o",
            "json",
          ]);
          if (result.exitCode !== 0) {
            return { cluster: id, data: {} };
          }
          return { cluster: id, data: parseConfigMapData(result.stdout) };
        },
        {
          intervals: [POLL_INTERVAL_MS],
          message: `ConfigMap ${CONFIG_MAP_NAME} in ${namespace} on ${id}`,
          timeout: OBJECT_WAIT_TIMEOUT_MS,
        },
      )
      .toMatchObject({ cluster: id, data: CONFIG_MAP_DATA });
  }

  async waitUntilNamespaceGone(id: string, namespace: string): Promise<void> {
    await this.#waitUntilObjectGone(id, ["get", "namespace", namespace]);
  }

  async waitUntilConfigMapGone(id: string, namespace: string): Promise<void> {
    await this.#waitUntilObjectGone(id, [
      "get",
      "configmap",
      CONFIG_MAP_NAME,
      "-n",
      namespace,
    ]);
  }

  async #waitUntilObjectGone(
    id: string,
    args: readonly string[],
  ): Promise<void> {
    await expect
      .poll(
        async () => {
          const result = await kubectlOnKind(hostKindClusterName(id), args);
          return {
            cluster: id,
            gone:
              result.exitCode !== 0 &&
              `${result.stdout}${result.stderr}`.includes("NotFound"),
          };
        },
        {
          intervals: [POLL_INTERVAL_MS],
          message: `${args.join(" ")} gone on ${id}`,
          timeout: OBJECT_WAIT_TIMEOUT_MS,
        },
      )
      .toEqual({ cluster: id, gone: true });
  }

  async delete(id: string): Promise<void> {
    if (this.#isPooledId(id)) {
      throw new Error(`refusing to delete shared Kind cluster ${id}`);
    }
    await this.#client.succeed(["resource", "delete", KIND_CLUSTER_TYPE, id]);
  }

  async #deleteBestEffort(id: string): Promise<void> {
    if (this.#isPooledId(id)) return;
    const result = await this.#client.run([
      "resource",
      "delete",
      KIND_CLUSTER_TYPE,
      id,
    ]);
    if (result.exitCode === 0 || isNotFound(result.stderr)) return;
    throw new Error(
      `failed to delete cluster ${id}${result.stderr.trim() ? `: ${result.stderr.trim()}` : ""}`,
    );
  }

  #isPooledId(id: string): boolean {
    return isPooledKindClusterId(id, this.#sandbox.kindIdPrefix);
  }

  async waitUntilGone(id: string): Promise<void> {
    await expect
      .poll(
        async () => {
          const [get, list] = await Promise.all([
            this.#client.run(["resource", "get", KIND_CLUSTER_TYPE, id]),
            this.#client.succeed(["resource", "list", KIND_CLUSTER_TYPE]),
          ]);
          const names = parseClusterList(list.stdout).map(({ name }) => name);
          return {
            listed: names.includes(clusterResourceName(id)),
            name: clusterResourceName(id),
            notFound: get.exitCode !== 0 && isNotFound(get.stderr),
          };
        },
        { intervals: [POLL_INTERVAL_MS], timeout: CLUSTER_WAIT_TIMEOUT_MS },
      )
      .toEqual({
        listed: false,
        name: clusterResourceName(id),
        notFound: true,
      });
  }

  async waitUntilHostClusterGone(id: string): Promise<void> {
    await expect
      .poll(() => kindNodeIDs(hostKindClusterName(id)), {
        intervals: [POLL_INTERVAL_MS],
        message: `host Kind nodes for ${id}`,
        timeout: CLUSTER_WAIT_TIMEOUT_MS,
      })
      .toEqual([]);
  }
}
