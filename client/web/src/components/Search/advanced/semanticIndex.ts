import type { Orama } from "@orama/orama";
import { create, insert, search } from "@orama/orama";

export interface SemanticEntry {
  term: string;
  label: string;
  expression: string;
  valueOnly: string;
  category: string;
}

const semanticSchema = {
  term: "string",
  label: "string",
  expression: "string",
  valueOnly: "string",
  category: "string",
} as const;

type SemanticDB = Orama<typeof semanticSchema>;

const ENTRIES: SemanticEntry[] = [
  // Resource types
  {
    term: "cluster kind",
    label: "Kind clusters",
    expression: 'resourceType == "kind.fleetshift.io/Cluster"',
    valueOnly: '"kind.fleetshift.io/Cluster"',
    category: "Resource Type",
  },
  {
    term: "cluster gcp hcp",
    label: "GCP HCP clusters",
    expression: 'resourceType == "gcphcp.fleetshift.io/Cluster"',
    valueOnly: '"gcphcp.fleetshift.io/Cluster"',
    category: "Resource Type",
  },
  {
    term: "node",
    label: "Kind nodes",
    expression: 'resourceType == "kind.fleetshift.io/Node"',
    valueOnly: '"kind.fleetshift.io/Node"',
    category: "Resource Type",
  },
  {
    term: "kubernetes object k8s",
    label: "Kubernetes objects",
    expression: 'resourceType == "kubernetes.fleetshift.io/Object"',
    valueOnly: '"kubernetes.fleetshift.io/Object"',
    category: "Resource Type",
  },

  // Common K8s kinds
  ...kindEntry("Pod"),
  ...kindEntry("Deployment"),
  ...kindEntry("Service"),
  ...kindEntry("ConfigMap"),
  ...kindEntry("Secret"),
  ...kindEntry("Namespace"),
  ...kindEntry("Node"),
  ...kindEntry("DaemonSet"),
  ...kindEntry("StatefulSet"),
  ...kindEntry("ReplicaSet"),
  ...kindEntry("Job"),
  ...kindEntry("ClusterRole"),
  ...kindEntry("ClusterRoleBinding"),
  ...kindEntry("Role"),
  ...kindEntry("RoleBinding"),
  ...kindEntry("ServiceAccount"),
  ...kindEntry("NetworkPolicy"),
  ...kindEntry("CustomResourceDefinition"),
  ...kindEntry("ClusterOperator"),
  ...kindEntry("Image"),
  ...kindEntry("ImageStream"),
  ...kindEntry("Template"),

  // Grouped kind queries
  {
    term: "workload workloads",
    label: "Workload resources",
    expression:
      'resource.observation.kind in ["Pod", "Deployment", "DaemonSet", "StatefulSet", "ReplicaSet"]',
    valueOnly:
      '["Pod", "Deployment", "DaemonSet", "StatefulSet", "ReplicaSet"]',
    category: "Kind Group",
  },
  {
    term: "networking network",
    label: "Networking resources",
    expression:
      'resource.observation.kind in ["Service", "NetworkPolicy", "Ingress"]',
    valueOnly: '["Service", "NetworkPolicy", "Ingress"]',
    category: "Kind Group",
  },
  {
    term: "rbac access roles permissions",
    label: "RBAC resources",
    expression:
      'resource.observation.kind in ["ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding", "ServiceAccount"]',
    valueOnly:
      '["ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding", "ServiceAccount"]',
    category: "Kind Group",
  },

  // Conditions
  {
    term: "ready healthy",
    label: "Ready resources",
    expression: 'resource.conditions.Ready.status == "True"',
    valueOnly: '"True"',
    category: "Condition",
  },
  {
    term: "not ready unhealthy broken",
    label: "Not ready resources",
    expression: 'resource.conditions.Ready.status == "False"',
    valueOnly: '"False"',
    category: "Condition",
  },
  {
    term: "available",
    label: "Available resources",
    expression: 'resource.conditions.Available.status == "True"',
    valueOnly: '"True"',
    category: "Condition",
  },
  {
    term: "degraded failing",
    label: "Degraded resources",
    expression: 'resource.conditions.Degraded.status != "False"',
    valueOnly: '"False"',
    category: "Condition",
  },
  {
    term: "progressing updating",
    label: "Progressing resources",
    expression: 'resource.conditions.Progressing.status == "True"',
    valueOnly: '"True"',
    category: "Condition",
  },

  // Extracted status
  {
    term: "running",
    label: "Running pods",
    expression: 'resource.observation.extracted.status == "Running"',
    valueOnly: '"Running"',
    category: "Status",
  },
  {
    term: "active",
    label: "Active namespaces",
    expression: 'resource.observation.extracted.phase == "Active"',
    valueOnly: '"Active"',
    category: "Status",
  },

  // Scopes
  {
    term: "namespaced scoped",
    label: "Namespace-scoped resources",
    expression: 'resource.observation.gvr.scope == "namespaced"',
    valueOnly: '"namespaced"',
    category: "Scope",
  },
  {
    term: "cluster scoped global",
    label: "Cluster-scoped resources",
    expression: 'resource.observation.gvr.scope == "cluster"',
    valueOnly: '"cluster"',
    category: "Scope",
  },

  // Common namespaces
  ...nsEntry("kube-system"),
  ...nsEntry("default"),
  ...nsEntry("openshift-monitoring"),
  ...nsEntry("openshift"),
  ...nsEntry("openshift-operators"),

  // API groups
  ...apiGroupEntry("apps", "Apps (Deployments, DaemonSets)"),
  ...apiGroupEntry("core", "Core (Pods, Services, ConfigMaps)"),
  ...apiGroupEntry("rbac.authorization.k8s.io", "RBAC (Roles, ClusterRoles)"),
  ...apiGroupEntry("networking.k8s.io", "Networking"),
  ...apiGroupEntry("batch", "Batch (Jobs, CronJobs)"),
];

function kindEntry(kind: string): SemanticEntry[] {
  return [
    {
      term: kind.toLowerCase(),
      label: `${kind} objects`,
      expression: `resource.observation.kind == "${kind}"`,
      valueOnly: `"${kind}"`,
      category: "Kind",
    },
  ];
}

function nsEntry(ns: string): SemanticEntry[] {
  return [
    {
      term: ns,
      label: `Namespace: ${ns}`,
      expression: `resource.observation.metadata.namespace == "${ns}"`,
      valueOnly: `"${ns}"`,
      category: "Namespace",
    },
  ];
}

function apiGroupEntry(group: string, label: string): SemanticEntry[] {
  return [
    {
      term: `${group} ${label.toLowerCase()}`,
      label: `API Group: ${label}`,
      expression: `resource.observation.gvr.group == "${group}"`,
      valueOnly: `"${group}"`,
      category: "API Group",
    },
  ];
}

let dbInstance: SemanticDB | undefined;

function getDB(): SemanticDB {
  if (dbInstance) return dbInstance;
  const db = create({ schema: semanticSchema });
  for (const entry of ENTRIES) {
    insert(db, {
      id: entry.expression,
      term: entry.term,
      label: entry.label,
      expression: entry.expression,
      valueOnly: entry.valueOnly,
      category: entry.category,
    });
  }
  dbInstance = db;
  return db;
}

export async function querySemantic(partial: string): Promise<SemanticEntry[]> {
  if (!partial.trim()) return [];

  const db = getDB();
  const result = await search(db, {
    term: partial.replace(/"/g, ""),
    threshold: 0.3,
    tolerance: 2,
    properties: ["term", "label", "category"],
    boost: { term: 10, label: 5, category: 2 },
    limit: 8,
  });

  return result.hits.map((hit) => {
    const doc = hit.document as unknown as SemanticEntry;
    return {
      term: doc.term,
      label: doc.label,
      expression: doc.expression,
      valueOnly: doc.valueOnly,
      category: doc.category,
    };
  });
}
