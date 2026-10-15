import { readFile } from "node:fs/promises";

import { flagString, type ParsedArgs } from "../../argv";

export interface ResourceRoute {
  collection: string;
  path: string;
  singular: string;
}

export function resourceRoute(type: string): ResourceRoute {
  const separator = type.indexOf("/");
  if (separator < 1 || separator === type.length - 1) {
    throw new Error(
      `resource type must use qualified form package/collection: ${type}`,
    );
  }
  const protoPackage = type.slice(0, separator);
  const collection = type.slice(separator + 1);
  const match = /^ocm\.resource_manager\.([a-z][a-z0-9]*)\.v1alpha$/.exec(
    protoPackage,
  );
  if (!match) {
    throw new Error(`unsupported resource package ${protoPackage}`);
  }
  const service = `${match[1]}.resource-manager.open-cluster-management.io`;
  return {
    collection,
    path: `/apis/${service}/v1alpha/${collection}`,
    singular: collection.endsWith("s") ? collection.slice(0, -1) : collection,
  };
}

export async function resourceSpec(
  path: string,
): Promise<Record<string, unknown>> {
  const content =
    path === "-" ? await readStdin() : await readFile(path, "utf8");
  return JSON.parse(content) as Record<string, unknown>;
}

async function readStdin(): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf8");
}

export function resourceID(args: ParsedArgs): string {
  const id = flagString(args, "id");
  if (!id) throw new Error("--id is required");
  return id;
}
