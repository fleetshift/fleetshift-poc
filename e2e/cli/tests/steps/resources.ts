import { expect } from "@playwright/test";

import { type FleetctlClient } from "../support/fleetctl";
import { KIND_CLUSTER_TYPE } from "./kind";

const KIND_NODE_TYPE = "kind.fleetshift.v1/nodes";
const KIND_CLUSTER_SERVICE = "kind.fleetshift.v1.ClusterService";
const KIND_NODE_SERVICE = "kind.fleetshift.v1.NodeService";
const KIND_CLUSTER_SPEC_TYPE = "KindClusterSpec";
const REQUIRED_METHODS = ["Create", "Get", "List", "Delete"] as const;
const REQUIRED_SPEC_FIELDS = ["nodes", "networking", "oidc", "role", "image"];

export interface ResourceTypeRow {
  service: string;
  singular: string;
  type: string;
}

export interface ResourceDescribeView {
  fieldNames: string[];
  methods: string[];
  specType: string;
  type: string;
}

export function parseResourceTypeRows(raw: string): ResourceTypeRow[] {
  const rows: ResourceTypeRow[] = [];
  for (const line of raw.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    if (/^TYPE\s+SINGULAR\s+SERVICE$/i.test(trimmed)) continue;
    if (/no extension resource types/i.test(trimmed)) continue;
    const parts = trimmed.split(/\s+/);
    if (parts.length < 3) continue;
    rows.push({
      service: parts.slice(2).join(" "),
      singular: parts[1],
      type: parts[0],
    });
  }
  return rows;
}

export function parseResourceDescribe(raw: string): ResourceDescribeView {
  const type = /^Type:\s+(\S+)/m.exec(raw)?.[1] ?? "";
  const specType = /^Spec\s+\(([^)]+)\):/m.exec(raw)?.[1] ?? "";
  const methods: string[] = [];
  const methodSection = /Methods:\n([\s\S]*?)(?:\n\s*\n|\nSpec\b)/.exec(raw);
  if (methodSection) {
    for (const line of methodSection[1].split("\n")) {
      const name = line.trim();
      if (name) methods.push(name);
    }
  }
  const fieldNames = [
    ...raw.matchAll(/(?:^|\s)(?:repeated |optional )?[\w.]+ (\w+) = \d+/g),
  ].map((match) => match[1]);
  return { fieldNames, methods, specType, type };
}

export class ResourceDiscoverySteps {
  readonly #client: FleetctlClient;

  constructor(client: FleetctlClient) {
    this.#client = client;
  }

  async typesIncludeKindClusterAndNode(): Promise<void> {
    const result = await this.#client.succeed(["resource", "types"]);
    const rows = parseResourceTypeRows(result.stdout);
    expect(
      rows.some(
        (row) =>
          row.type === KIND_CLUSTER_TYPE &&
          row.service === KIND_CLUSTER_SERVICE,
      ),
      `Kind Cluster type in:\n${result.stdout}`,
    ).toBe(true);
    expect(
      rows.some(
        (row) =>
          row.type === KIND_NODE_TYPE && row.service === KIND_NODE_SERVICE,
      ),
      `Kind Node type in:\n${result.stdout}`,
    ).toBe(true);
  }

  async expectKindClusterSchema(): Promise<void> {
    const result = await this.#client.succeed([
      "resource",
      "describe",
      KIND_CLUSTER_TYPE,
    ]);
    const described = parseResourceDescribe(result.stdout);
    expect(described.type).toBe(KIND_CLUSTER_TYPE);
    for (const method of REQUIRED_METHODS) {
      expect(
        described.methods.some((name) => name.includes(method)),
        `method ${method} in ${described.methods.join(", ")}`,
      ).toBe(true);
    }
    expect(described.specType).toContain(KIND_CLUSTER_SPEC_TYPE);
    for (const field of REQUIRED_SPEC_FIELDS) {
      expect(
        described.fieldNames,
        `spec field ${field} in ${described.fieldNames.join(", ")}`,
      ).toContain(field);
    }
  }
}
