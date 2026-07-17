import type { InventoryResource, SearchResultRender } from "@fleetshift/common";

const CANONICAL_RE =
  /\/\/kubernetes\.fleetshift\.io\/clusters\/([^/]+)\/apiResources\/([^/]+)\/objects\/([^/]+)/;

export function resolveK8sObject(
  resource: InventoryResource,
): SearchResultRender {
  const obs = (resource.resource as Record<string, unknown>).observation as
    | {
        kind?: string;
        metadata?: { name?: string; namespace?: string; uid?: string };
      }
    | undefined;

  const kind = obs?.kind ?? "Object";
  const metaName = obs?.metadata?.name;
  const namespace = obs?.metadata?.namespace;
  const uid = obs?.metadata?.uid ?? resource.resource.uid;

  const match = resource.name.match(CANONICAL_RE);
  const clusterId = match?.[1] ?? "";

  const title = metaName ?? resource.resource.name.split("/").pop() ?? "";

  const namespaceSuffix = namespace ? `${namespace} · ` : "";

  switch (kind) {
    case "Pod":
      return {
        scope: "core-plugin",
        module: "PodsModule",
        to: `${clusterId}/${uid}`,
        title,
        description: `${namespaceSuffix}Pod`,
      };
    case "Namespace":
      return {
        scope: "core-plugin",
        module: "NamespacesModule",
        to: `${clusterId}/${uid}`,
        title,
        description: "Namespace",
      };
    case "Node":
      return {
        scope: "core-plugin",
        module: "NodesModule",
        to: `${clusterId}/${uid}`,
        title,
        description: "Node",
      };
    default:
      return {
        scope: "core-plugin",
        module: "ClustersModule",
        to: clusterId,
        title,
        description: `${namespaceSuffix}${kind}`,
      };
  }
}

export { OutlinedHddIcon as K8sObjectIcon } from "@patternfly/react-icons";
