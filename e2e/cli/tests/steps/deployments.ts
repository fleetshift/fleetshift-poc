import { randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import { expect } from "@playwright/test";

import { type CleanupStack } from "../support/cleanup";
import { isNotFound } from "../support/command";
import { type FleetctlClient } from "../support/fleetctl";
import { parseJSON } from "../support/json";
import { type Sandbox } from "../support/sandbox";

const WAIT_TIMEOUT_MS = 2 * 60_000;
const POLL_INTERVAL_MS = 2_000;
const KUBERNETES_RESOURCE_TYPE = "kubernetes";
export const CONFIG_MAP_NAME = "test-config";
export const CONFIG_MAP_DATA = { from: "fleetshift-e2e-cli" } as const;

export interface DeploymentView {
  name: string;
  pauseReason: string;
  state: string;
}

function deploymentName(id: string): string {
  return id.startsWith("deployments/") ? id : `deployments/${id}`;
}

function deploymentView(
  value: Partial<DeploymentView> | null | undefined,
): DeploymentView {
  return {
    name: value?.name ?? "",
    pauseReason: value?.pauseReason ?? "",
    state: value?.state ?? "",
  };
}

export function parseDeployment(raw: string): DeploymentView {
  return deploymentView(parseJSON(raw));
}

export function parseDeploymentList(raw: string): DeploymentView[] {
  const value = parseJSON<unknown>(raw);
  if (!Array.isArray(value)) throw new Error("invalid JSON: expected array");
  return value.map((item) => deploymentView(item as Partial<DeploymentView>));
}

export function deploymentTerminalFailure(dep: DeploymentView): string | null {
  if (dep.state === "STATE_FAILED") {
    return `deployment ${dep.name} STATE_FAILED${dep.pauseReason ? `: ${dep.pauseReason}` : ""}`;
  }
  if (dep.pauseReason.trim()) {
    return `deployment ${dep.name} paused (${dep.state}): ${dep.pauseReason}`;
  }
  return null;
}

export function uniqueId(prefix: string): string {
  if (!prefix.trim()) throw new Error("unique ID prefix is required");
  return `${prefix}-${randomUUID().replaceAll("-", "").slice(0, 8)}`;
}

export class DeploymentSteps {
  readonly #cleanup: CleanupStack;
  readonly #client: FleetctlClient;
  readonly #sandbox: Sandbox;

  constructor(client: FleetctlClient, sandbox: Sandbox, cleanup: CleanupStack) {
    this.#client = client;
    this.#sandbox = sandbox;
    this.#cleanup = cleanup;
  }

  async waitUntilListed(id: string): Promise<void> {
    await expect
      .poll(
        async () => {
          const result = await this.#client.succeed(["deployment", "list"]);
          return parseDeploymentList(result.stdout).map(({ name }) => name);
        },
        { intervals: [POLL_INTERVAL_MS], timeout: WAIT_TIMEOUT_MS },
      )
      .toContain(deploymentName(id));
  }

  async waitUntilActive(id: string): Promise<void> {
    await this.#waitUntilActive(id, deploymentTerminalFailure);
  }

  async waitUntilPausedForDeliveryAuth(id: string): Promise<void> {
    await expect
      .poll(
        async () => {
          const result = await this.#client.succeed(["deployment", "get", id]);
          const deployment = parseDeployment(result.stdout);
          if (deployment.state === "STATE_FAILED") {
            throw new Error(
              `deployment ${deployment.name} STATE_FAILED before pause`,
            );
          }
          return deployment.pauseReason;
        },
        { intervals: [POLL_INTERVAL_MS], timeout: WAIT_TIMEOUT_MS },
      )
      .toContain("delivery auth failed");
  }

  async waitUntilResumed(id: string): Promise<void> {
    await this.#waitUntilActive(id, (deployment) => {
      if (deployment.state !== "STATE_FAILED") return null;
      return `deployment ${deployment.name} STATE_FAILED${deployment.pauseReason ? `: ${deployment.pauseReason}` : ""}`;
    });
  }

  async #waitUntilActive(
    id: string,
    terminalFailure: (deployment: DeploymentView) => string | null,
  ): Promise<void> {
    await expect
      .poll(
        async () => {
          const result = await this.#client.succeed(["deployment", "get", id]);
          const deployment = parseDeployment(result.stdout);
          expect(deployment.name).toBe(deploymentName(id));
          const terminal = terminalFailure(deployment);
          if (terminal) throw new Error(terminal);
          return {
            pauseReason: deployment.pauseReason,
            state: deployment.state,
          };
        },
        { intervals: [POLL_INTERVAL_MS], timeout: WAIT_TIMEOUT_MS },
      )
      .toEqual({ pauseReason: "", state: "STATE_ACTIVE" });
  }

  async createNamespace(options: {
    id: string;
    namespace: string;
    targets: readonly string[];
  }): Promise<void> {
    await this.#create(
      options.id,
      options.targets,
      {
        apiVersion: "v1",
        kind: "Namespace",
        metadata: { name: options.namespace },
      },
      this.#client.configDir,
    );
  }

  async createConfigMap(options: {
    configDir?: string;
    id: string;
    namespace: string;
    targets: readonly string[];
  }): Promise<void> {
    await this.#create(
      options.id,
      options.targets,
      {
        apiVersion: "v1",
        data: CONFIG_MAP_DATA,
        kind: "ConfigMap",
        metadata: { name: CONFIG_MAP_NAME, namespace: options.namespace },
      },
      options.configDir ?? this.#client.configDir,
    );
  }

  async #create(
    id: string,
    targets: readonly string[],
    manifest: object,
    configDir: string,
  ): Promise<void> {
    expect(targets.length).toBeGreaterThan(0);
    const dir = path.join(this.#sandbox.workDir, `manifest-${randomUUID()}`);
    await mkdir(dir, { mode: 0o700 });
    const manifestFile = path.join(dir, "manifest.json");
    await writeFile(manifestFile, JSON.stringify(manifest), { mode: 0o600 });
    await this.#client.succeed(
      [
        "deployment",
        "create",
        "--id",
        id,
        "--manifest-file",
        manifestFile,
        "--resource-type",
        KUBERNETES_RESOURCE_TYPE,
        "--placement-type",
        "static",
        "--target-ids",
        targets.map((target) => `k8s-${target}`).join(","),
      ],
      { configDir },
    );
    this.#cleanup.add(() => this.#deleteBestEffort(id));
  }

  async resume(id: string): Promise<void> {
    await this.#client.succeed(["deployment", "resume", id]);
  }

  async delete(id: string): Promise<void> {
    await this.#client.succeed(["deployment", "delete", id]);
  }

  async #deleteBestEffort(id: string): Promise<void> {
    const result = await this.#client.run(["deployment", "delete", id]);
    if (result.exitCode === 0 || isNotFound(result.stderr)) return;
    throw new Error(
      `failed to delete deployment ${id}${result.stderr.trim() ? `: ${result.stderr.trim()}` : ""}`,
    );
  }

  async waitUntilGone(id: string): Promise<void> {
    const wanted = deploymentName(id);
    await expect
      .poll(
        async () => {
          const [get, list] = await Promise.all([
            this.#client.run(["deployment", "get", id]),
            this.#client.succeed(["deployment", "list"]),
          ]);
          const names = parseDeploymentList(list.stdout).map(
            ({ name }) => name,
          );
          return {
            listed: names.includes(wanted),
            name: wanted,
            notFound: get.exitCode !== 0 && isNotFound(get.stderr),
          };
        },
        { intervals: [POLL_INTERVAL_MS], timeout: WAIT_TIMEOUT_MS },
      )
      .toEqual({ listed: false, name: wanted, notFound: true });
  }
}
