import type { NodepoolEntry } from "./CreateGcpHcpWizard";

export const DEFAULT_NODEPOOL: NodepoolEntry = {
  id: "",
  replicas: 2,
  instanceType: "n1-standard-4",
  rootVolumeSize: 128,
  rootVolumeType: "pd-standard",
  autoRepair: true,
  upgradeType: "Replace",
};

export const INSTANCE_TYPES = [
  "n1-standard-4",
  "n1-standard-8",
  "n1-standard-16",
  "n2-standard-4",
  "n2-standard-8",
] as const;

export const VOLUME_TYPES = ["pd-standard", "pd-ssd"] as const;

export const UPGRADE_TYPES = [
  { value: "Replace", label: "Replace" },
  { value: "InPlace", label: "In-place" },
] as const;

export function serializeToYaml(pools: NodepoolEntry[]): string {
  return pools
    .map((p) =>
      [
        `- id: ${p.id || '""'}`,
        `  replicas: ${p.replicas}`,
        `  instanceType: ${p.instanceType}`,
        `  rootVolumeSize: ${p.rootVolumeSize}`,
        `  rootVolumeType: ${p.rootVolumeType}`,
        `  autoRepair: ${p.autoRepair}`,
        `  upgradeType: ${p.upgradeType}`,
      ].join("\n"),
    )
    .join("\n");
}

export function parseFromYaml(text: string): NodepoolEntry[] | null {
  try {
    const pools: NodepoolEntry[] = [];
    let current: Partial<NodepoolEntry> | null = null;

    for (const rawLine of text.split("\n")) {
      const line = rawLine.trimEnd();
      if (!line.trim()) continue;

      if (line.startsWith("- ")) {
        if (current) pools.push(finishPool(current));
        current = {};
        if (!parseField(current, line.slice(2))) return null;
      } else if (line.startsWith("  ") && current) {
        if (!parseField(current, line.trim())) return null;
      } else {
        return null;
      }
    }
    if (current) pools.push(finishPool(current));
    return pools.length > 0 ? pools : null;
  } catch {
    return null;
  }
}

function parseField(pool: Partial<NodepoolEntry>, field: string): boolean {
  const colonIdx = field.indexOf(":");
  if (colonIdx === -1) return false;
  const key = field.slice(0, colonIdx).trim();
  let val = field.slice(colonIdx + 1).trim();

  if (val.startsWith('"') && val.endsWith('"')) {
    val = val.slice(1, -1);
  }

  switch (key) {
    case "id":
      pool.id = val;
      return true;
    case "replicas": {
      const parsed = parseInt(val, 10);
      if (Number.isNaN(parsed)) return false;
      pool.replicas = parsed;
      return true;
    }
    case "instanceType":
      pool.instanceType = val;
      return true;
    case "rootVolumeSize": {
      const parsed = parseInt(val, 10);
      if (Number.isNaN(parsed)) return false;
      pool.rootVolumeSize = parsed;
      return true;
    }
    case "rootVolumeType":
      pool.rootVolumeType = val;
      return true;
    case "autoRepair":
      pool.autoRepair = val === "true";
      return true;
    case "upgradeType":
      pool.upgradeType = val;
      return true;
    default:
      return false;
  }
}

function finishPool(partial: Partial<NodepoolEntry>): NodepoolEntry {
  return { ...DEFAULT_NODEPOOL, ...partial };
}

const VALID_INSTANCE_TYPES: Set<string> = new Set(INSTANCE_TYPES);
const VALID_VOLUME_TYPES: Set<string> = new Set(VOLUME_TYPES);
const VALID_UPGRADE_TYPES: Set<string> = new Set(
  UPGRADE_TYPES.map((t) => t.value),
);

export function validatePools(pools: NodepoolEntry[]): string[] {
  const errors: string[] = [];
  const ids = new Set<string>();

  pools.forEach((pool, i) => {
    const label = pool.id || `pool ${i + 1}`;

    if (pool.id && !/^[a-z][-a-z0-9]*$/.test(pool.id)) {
      errors.push(
        `${label}: ID must start with a lowercase letter and contain only a-z, 0-9, hyphens`,
      );
    }

    if (pool.id && ids.has(pool.id)) {
      errors.push(`${label}: duplicate pool ID`);
    }
    if (pool.id) ids.add(pool.id);

    if (pool.replicas < 1) {
      errors.push(`${label}: replicas must be at least 1`);
    }

    if (!VALID_INSTANCE_TYPES.has(pool.instanceType)) {
      errors.push(`${label}: unknown instance type "${pool.instanceType}"`);
    }

    if (pool.rootVolumeSize < 1) {
      errors.push(`${label}: root volume size must be at least 1`);
    }

    if (!VALID_VOLUME_TYPES.has(pool.rootVolumeType)) {
      errors.push(`${label}: unknown volume type "${pool.rootVolumeType}"`);
    }

    if (!VALID_UPGRADE_TYPES.has(pool.upgradeType)) {
      errors.push(`${label}: upgrade type must be "Replace" or "InPlace"`);
    }
  });

  return errors;
}
