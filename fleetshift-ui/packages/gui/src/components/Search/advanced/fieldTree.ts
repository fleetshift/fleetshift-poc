import type { FieldNode } from "./types";

const CONDITION_NAMES = [
  "Applied",
  "Approved",
  "Available",
  "ContainersReady",
  "Dangling",
  "Degraded",
  "DiskPressure",
  "Established",
  "Failed",
  "Failing",
  "Initialized",
  "MemoryPressure",
  "NamesAccepted",
  "PIDPressure",
  "PodReadyToStartContainers",
  "PodScheduled",
  "Progressing",
  "Ready",
  "Succeeded",
  "Upgradeable",
  "Valid",
];

const CONDITION_LEAF_CHILDREN: FieldNode[] = [
  {
    segment: "status",
    path: "",
    label: "Status",
    type: "string",
    enumValues: ["True", "False", "Unknown"],
  },
  { segment: "reason", path: "", label: "Reason", type: "string" },
  { segment: "message", path: "", label: "Message", type: "string" },
  {
    segment: "lastTransitionTime",
    path: "",
    label: "Last Transition",
    type: "string",
  },
];

function conditionNode(name: string): FieldNode {
  const basePath = `resource.conditions.${name}`;
  return {
    segment: name,
    path: basePath,
    label: name,
    children: CONDITION_LEAF_CHILDREN.map((c) => ({
      ...c,
      path: `${basePath}.${c.segment}`,
    })),
  };
}

const RESOURCE_TYPE_VALUES = [
  "gcphcp.fleetshift.io/Cluster",
  "kind.fleetshift.io/Cluster",
  "kind.fleetshift.io/Node",
  "kubernetes.fleetshift.io/Object",
];

const KIND_VALUES = [
  "APIService",
  "CertificateSigningRequest",
  "ClusterOperator",
  "ClusterRole",
  "ClusterRoleBinding",
  "ConfigMap",
  "ControllerRevision",
  "CustomResourceDefinition",
  "DaemonSet",
  "Deployment",
  "FlowSchema",
  "IPAddress",
  "Image",
  "ImageStream",
  "Job",
  "Namespace",
  "NetworkPolicy",
  "Node",
  "Pod",
  "PriorityLevelConfiguration",
  "PrometheusRule",
  "ReplicaSet",
  "Role",
  "RoleBinding",
  "Secret",
  "SecurityContextConstraints",
  "Service",
  "ServiceAccount",
  "ServiceMonitor",
  "StatefulSet",
  "Template",
  "ValidatingAdmissionPolicy",
  "ValidatingAdmissionPolicyBinding",
];

const NAMESPACE_VALUES = [
  "default",
  "kube-node-lease",
  "kube-public",
  "kube-system",
  "local-path-storage",
  "openshift",
  "openshift-authentication",
  "openshift-cluster-version",
  "openshift-config",
  "openshift-config-managed",
  "openshift-dns",
  "openshift-etcd",
  "openshift-infra",
  "openshift-marketplace",
  "openshift-monitoring",
  "openshift-multus",
  "openshift-network-operator",
  "openshift-operators",
  "openshift-ovn-kubernetes",
];

const API_GROUP_VALUES = [
  "admissionregistration.k8s.io",
  "apiextensions.k8s.io",
  "apiregistration.k8s.io",
  "apps",
  "batch",
  "certificates.k8s.io",
  "config.openshift.io",
  "core",
  "flowcontrol.apiserver.k8s.io",
  "image.openshift.io",
  "monitoring.coreos.com",
  "networking.k8s.io",
  "operator.openshift.io",
  "rbac.authorization.k8s.io",
  "security.openshift.io",
  "storage.k8s.io",
  "template.openshift.io",
];

export const FIELD_TREE: FieldNode[] = [
  {
    segment: "name",
    path: "name",
    label: "Resource Name",
    description: "Full resource URI",
    type: "string",
  },
  {
    segment: "resourceType",
    path: "resourceType",
    label: "Resource Type",
    description: "FleetShift resource type",
    type: "string",
    enumValues: RESOURCE_TYPE_VALUES,
  },
  {
    segment: "resource",
    path: "resource",
    label: "Resource",
    description: "Resource data fields",
    children: [
      {
        segment: "conditions",
        path: "resource.conditions",
        label: "Conditions",
        description: "Health and status conditions",
        children: CONDITION_NAMES.map(conditionNode),
        container: true,
      },
      {
        segment: "observation",
        path: "resource.observation",
        label: "Observation",
        description: "Observed K8s object state",
        children: [
          {
            segment: "kind",
            path: "resource.observation.kind",
            label: "Kind",
            description: "Kubernetes object type",
            type: "string",
            enumValues: KIND_VALUES,
          },
          {
            segment: "apiVersion",
            path: "resource.observation.apiVersion",
            label: "API Version",
            type: "string",
          },
          {
            segment: "gvr",
            path: "resource.observation.gvr",
            label: "GVR",
            description: "Group/Version/Resource",
            children: [
              {
                segment: "group",
                path: "resource.observation.gvr.group",
                label: "API Group",
                type: "string",
                enumValues: API_GROUP_VALUES,
              },
              {
                segment: "version",
                path: "resource.observation.gvr.version",
                label: "API Version",
                type: "string",
                enumValues: ["v1"],
              },
              {
                segment: "resource",
                path: "resource.observation.gvr.resource",
                label: "Resource Name",
                type: "string",
              },
              {
                segment: "scope",
                path: "resource.observation.gvr.scope",
                label: "Scope",
                description: "Cluster or namespace scoped",
                type: "string",
                enumValues: ["cluster", "namespaced"],
              },
            ],
          },
          {
            segment: "metadata",
            path: "resource.observation.metadata",
            label: "Metadata",
            description: "K8s object metadata",
            children: [
              {
                segment: "name",
                path: "resource.observation.metadata.name",
                label: "Object Name",
                type: "string",
              },
              {
                segment: "namespace",
                path: "resource.observation.metadata.namespace",
                label: "Namespace",
                type: "string",
                enumValues: NAMESPACE_VALUES,
              },
              {
                segment: "uid",
                path: "resource.observation.metadata.uid",
                label: "UID",
                type: "string",
              },
              {
                segment: "creationTimestamp",
                path: "resource.observation.metadata.creationTimestamp",
                label: "Created",
                type: "string",
              },
            ],
          },
          {
            segment: "extracted",
            path: "resource.observation.extracted",
            label: "Extracted",
            description: "Kind-specific extracted fields",
            children: [
              {
                segment: "status",
                path: "resource.observation.extracted.status",
                label: "Status",
                type: "string",
                enumValues: ["Running"],
              },
              {
                segment: "phase",
                path: "resource.observation.extracted.phase",
                label: "Phase",
                type: "string",
                enumValues: ["Active", "Running"],
              },
              {
                segment: "replicas",
                path: "resource.observation.extracted.replicas",
                label: "Replicas",
                type: "number",
              },
              {
                segment: "readyReplicas",
                path: "resource.observation.extracted.readyReplicas",
                label: "Ready Replicas",
                type: "number",
              },
              {
                segment: "nodeName",
                path: "resource.observation.extracted.nodeName",
                label: "Node Name",
                type: "string",
              },
              {
                segment: "podIP",
                path: "resource.observation.extracted.podIP",
                label: "Pod IP",
                type: "string",
              },
              {
                segment: "clusterIP",
                path: "resource.observation.extracted.clusterIP",
                label: "Cluster IP",
                type: "string",
              },
              {
                segment: "type",
                path: "resource.observation.extracted.type",
                label: "Service Type",
                type: "string",
                enumValues: ["ClusterIP", "NodePort", "LoadBalancer"],
              },
              {
                segment: "role",
                path: "resource.observation.extracted.role",
                label: "Node Role",
                type: "string",
                enumValues: ["control-plane", "worker"],
              },
            ],
          },
        ],
      },
      {
        segment: "localLabels",
        path: "resource.localLabels",
        label: "Labels",
        description: "Resource labels (dynamic keys)",
        type: "string",
        container: true,
      },
      {
        segment: "uid",
        path: "resource.uid",
        label: "Resource UID",
        type: "string",
      },
      {
        segment: "createTime",
        path: "resource.createTime",
        label: "Created",
        type: "string",
      },
      {
        segment: "updateTime",
        path: "resource.updateTime",
        label: "Updated",
        type: "string",
      },
    ],
  },
];

const TOP_LEVEL_SHORTCUTS: FieldNode[] = [
  {
    segment: "resource.observation.kind",
    path: "resource.observation.kind",
    label: "Kind",
    description: "Kubernetes object type",
    type: "string",
    enumValues: KIND_VALUES,
  },
  {
    segment: "resource.observation.metadata.namespace",
    path: "resource.observation.metadata.namespace",
    label: "Namespace",
    description: "K8s namespace",
    type: "string",
    enumValues: NAMESPACE_VALUES,
  },
  {
    segment: "resource.conditions",
    path: "resource.conditions",
    label: "Conditions",
    description: "Resource health conditions",
    children: CONDITION_NAMES.map(conditionNode),
  },
  {
    segment: "resource.observation.gvr.group",
    path: "resource.observation.gvr.group",
    label: "API Group",
    type: "string",
    enumValues: API_GROUP_VALUES,
  },
  {
    segment: "resource.observation.gvr.scope",
    path: "resource.observation.gvr.scope",
    label: "Scope",
    description: "Cluster or namespace scoped",
    type: "string",
    enumValues: ["cluster", "namespaced"],
  },
  {
    segment: "resourceType",
    path: "resourceType",
    label: "Resource Type",
    description: "FleetShift resource type",
    type: "string",
    enumValues: RESOURCE_TYPE_VALUES,
  },
  {
    segment: "name",
    path: "name",
    label: "Resource Name",
    description: "Full resource URI",
    type: "string",
  },
];

function findInTree(
  nodes: FieldNode[],
  segments: string[],
): FieldNode | undefined {
  if (segments.length === 0) return undefined;
  const [head, ...rest] = segments;
  const match = nodes.find((n) => n.segment === head);
  if (!match) return undefined;
  if (rest.length === 0) return match;
  if (match.children) return findInTree(match.children, rest);
  return undefined;
}

export function getTopLevelShortcuts(): FieldNode[] {
  return TOP_LEVEL_SHORTCUTS;
}

export function getTopLevelNodes(): FieldNode[] {
  return FIELD_TREE;
}

export function getChildrenAt(dottedPath: string): FieldNode[] {
  const clean = dottedPath.endsWith(".") ? dottedPath.slice(0, -1) : dottedPath;
  const segments = clean.split(".");
  const node = findInTree(FIELD_TREE, segments);
  return node?.children ?? [];
}

export function getNodeAt(dottedPath: string): FieldNode | undefined {
  const segments = dottedPath.split(".");
  return findInTree(FIELD_TREE, segments);
}

export function isLeaf(dottedPath: string): boolean {
  const node = getNodeAt(dottedPath);
  if (!node) return false;
  return !node.children;
}

export function getAllLeaves(nodes: FieldNode[] = FIELD_TREE): FieldNode[] {
  const result: FieldNode[] = [];
  for (const node of nodes) {
    if (node.children) {
      result.push(...getAllLeaves(node.children));
    } else {
      result.push(node);
    }
  }
  return result;
}
