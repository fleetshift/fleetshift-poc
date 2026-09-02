import { mkdtemp, stat } from "node:fs/promises";
import path from "node:path";

import { expect } from "@playwright/test";

import { type FleetctlClient } from "../support/fleetctl";
import { type Sandbox } from "../support/sandbox";

export function tokenEmail(raw: string): string {
  const value: unknown = JSON.parse(raw);
  if (typeof value !== "object" || value === null) {
    throw new Error("malformed inspect-token JSON");
  }
  const tokens = value as Record<string, unknown>;
  for (const key of ["access_token", "id_token"]) {
    const token = tokens[key];
    if (
      typeof token === "object" &&
      token !== null &&
      "claims" in token &&
      typeof token.claims === "object" &&
      token.claims !== null
    ) {
      const claims = token.claims as Record<string, unknown>;
      if (typeof claims["email"] === "string" && claims["email"].trim()) {
        return claims["email"];
      }
    }
  }
  throw new Error("inspect-token JSON has no email claim");
}

export class LoginSteps {
  readonly #client: FleetctlClient;
  readonly #sandbox: Sandbox;

  constructor(client: FleetctlClient, sandbox: Sandbox) {
    this.#client = client;
    this.#sandbox = sandbox;
  }

  async tokenBelongsTo(
    email: string,
    configDir = this.#client.configDir,
  ): Promise<void> {
    const result = await this.#client.succeed(["auth", "inspect-token"], {
      configDir,
    });
    expect(tokenEmail(result.stdout)).toBe(email);
  }

  async listsDeployments(configDir: string): Promise<void> {
    await this.#client.succeed(["deployment", "list"], { configDir });
  }

  async credentialsAreIsolated(): Promise<void> {
    await stat(path.join(this.#client.configDir, "credentials.json"));
    const empty = await mkdtemp(
      path.join(this.#sandbox.workDir, "empty-config-"),
    );
    const result = await this.#client.run(["deployment", "list"], {
      configDir: empty,
    });
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr.toLowerCase()).toContain("unauthenticated");
  }
}
