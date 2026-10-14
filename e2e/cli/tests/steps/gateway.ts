import { expect } from "@playwright/test";

import { type FleetctlClient } from "../support/fleetctl";
import { requestWithCAFile } from "../support/https";
import { type Sandbox } from "../support/sandbox";

function parseDeploymentNames(raw: Buffer): string[] {
  const value: unknown = JSON.parse(raw.toString("utf8"));
  if (
    typeof value !== "object" ||
    value === null ||
    !("deployments" in value) ||
    !Array.isArray(value.deployments)
  ) {
    throw new Error("malformed gateway deployment list JSON");
  }
  return value.deployments.map((deployment) => {
    if (
      typeof deployment !== "object" ||
      deployment === null ||
      !("name" in deployment) ||
      typeof deployment.name !== "string"
    ) {
      throw new Error("gateway deployment has no name");
    }
    return deployment.name;
  });
}

export class GatewaySteps {
  readonly #client: FleetctlClient;
  readonly #sandbox: Sandbox;

  constructor(client: FleetctlClient, sandbox: Sandbox) {
    this.#client = client;
    this.#sandbox = sandbox;
  }

  async servesHealth(path: "/livez" | "/readyz"): Promise<void> {
    const response = await requestWithCAFile(
      `${this.#sandbox.uiOrigin}${path}`,
      this.#sandbox.caFile,
    );
    expect(response.status, `${path} status`).toBe(200);
    expect(response.body.toString("utf8").trim(), `${path} body`).toBe("ok");
  }

  async rejectsDeploymentList(token?: string): Promise<void> {
    const response = await requestWithCAFile(
      `${this.#sandbox.uiOrigin}/v1/deployments`,
      this.#sandbox.caFile,
      {
        headers: token ? { Authorization: `Bearer ${token}` } : undefined,
      },
    );
    expect(response.status).toBe(401);
  }

  async listsDeployment(id: string): Promise<void> {
    const token = await this.#client.accessToken();
    const response = await requestWithCAFile(
      `${this.#sandbox.uiOrigin}/v1/deployments`,
      this.#sandbox.caFile,
      { headers: { Authorization: `Bearer ${token}` } },
    );
    expect(response.status).toBe(200);
    expect(parseDeploymentNames(response.body)).toContain(`deployments/${id}`);
  }
}
