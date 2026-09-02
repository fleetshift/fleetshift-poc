import { randomUUID } from "node:crypto";

// The Kind addon adds the `fs--` ownership prefix before handing this ID to
// Kind, whose practical cluster-name limit is 50 characters.
export const MAX_KIND_RESOURCE_ID_LENGTH = 46;

const UNIQUE_SUFFIX_LENGTH = 8;
const RFC1123_PREFIX = /^[a-z0-9]([a-z0-9-]*)?$/;

/**
 * Builds an RFC1123-safe, sandbox-unique Kind resource ID that the Kind addon
 * can encode without exceeding its provider cluster-name limit.
 */
export function uniqueKindClusterId(prefix: string): string {
  if (!RFC1123_PREFIX.test(prefix)) {
    throw new Error(
      `FLEETSHIFT_KIND_PREFIX is not RFC1123-safe: ${JSON.stringify(prefix)} must start with a lowercase letter or digit and contain only lowercase letters, digits, and "-"`,
    );
  }
  const id = `${prefix}${randomUUID().replaceAll("-", "").slice(0, UNIQUE_SUFFIX_LENGTH)}`;
  if (id.length > MAX_KIND_RESOURCE_ID_LENGTH) {
    throw new Error(
      `FLEETSHIFT_KIND_PREFIX is too long: ${JSON.stringify(prefix)} leaves no room for a unique Kind resource ID (maximum ${MAX_KIND_RESOURCE_ID_LENGTH} characters)`,
    );
  }
  return id;
}

export function uniqueKindClusterIdFromEnv(): string {
  const prefix = process.env["FLEETSHIFT_KIND_PREFIX"];
  if (!prefix) throw new Error("FLEETSHIFT_KIND_PREFIX is unset");
  return uniqueKindClusterId(prefix);
}
