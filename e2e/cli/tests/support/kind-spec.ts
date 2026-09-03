export type KindNodeRole = "control-plane" | "worker";

export interface KindNodeSpec {
  image?: string;
  role: KindNodeRole;
}

export interface KindClusterCreateSpec {
  nodes?: readonly KindNodeSpec[];
}

const NODE_ROLES = new Set<KindNodeRole>(["control-plane", "worker"]);

/** Stable identity for a create spec. Node order is significant. */
export function kindClusterSpecKey(spec: KindClusterCreateSpec): string {
  return JSON.stringify(canonicalizeKindClusterSpec(spec));
}

export function isDefaultKindClusterSpec(spec: KindClusterCreateSpec): boolean {
  return kindClusterSpecKey(spec) === kindClusterSpecKey({});
}

export function kindClusterSpecsEqual(
  left: KindClusterCreateSpec,
  right: KindClusterCreateSpec,
): boolean {
  return kindClusterSpecKey(left) === kindClusterSpecKey(right);
}

/** Create-spec fields from a cluster view spec, ignoring extras such as name. */
export function kindClusterCreateSpecFromView(
  spec: Record<string, unknown>,
): KindClusterCreateSpec {
  return parseKindClusterCreateSpec(
    spec.nodes === undefined ? {} : { nodes: spec.nodes },
  );
}

export function parseKindClusterCreateSpec(
  value: unknown,
): KindClusterCreateSpec {
  if (!isPlainObject(value)) {
    throw new Error("kind cluster spec is malformed");
  }
  for (const key of Object.keys(value)) {
    if (key !== "nodes") {
      throw new Error("kind cluster spec is malformed");
    }
  }
  if (value.nodes === undefined) return {};
  if (!Array.isArray(value.nodes)) {
    throw new Error("kind cluster spec is malformed");
  }
  return { nodes: value.nodes.map(parseKindNodeSpec) };
}

function parseKindNodeSpec(value: unknown): KindNodeSpec {
  if (!isPlainObject(value)) {
    throw new Error("kind cluster spec is malformed");
  }
  for (const key of Object.keys(value)) {
    if (key !== "role" && key !== "image") {
      throw new Error("kind cluster spec is malformed");
    }
  }
  if (!isNodeRole(value.role)) {
    throw new Error("kind cluster spec is malformed");
  }
  if (value.image !== undefined && typeof value.image !== "string") {
    throw new Error("kind cluster spec is malformed");
  }
  const node: KindNodeSpec = { role: value.role };
  if (typeof value.image === "string") node.image = value.image;
  return node;
}

function canonicalizeKindClusterSpec(
  spec: KindClusterCreateSpec,
): Record<string, unknown> {
  if (spec.nodes === undefined) return {};
  return { nodes: spec.nodes.map(canonicalizeKindNodeSpec) };
}

function canonicalizeKindNodeSpec(node: KindNodeSpec): Record<string, unknown> {
  const canonical: Record<string, unknown> = { role: node.role };
  if (node.image !== undefined) canonical.image = node.image;
  return canonical;
}

function isNodeRole(value: unknown): value is KindNodeRole {
  return typeof value === "string" && NODE_ROLES.has(value as KindNodeRole);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
