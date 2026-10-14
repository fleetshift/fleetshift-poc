import type { InventoryResource, SearchResultRender } from "@fleetshift/common";

export function resolveKindCluster(
  resource: InventoryResource,
): SearchResultRender {
  const clusterId =
    resource.resource.name.split("/").pop() ?? resource.resource.name;
  return {
    scope: "core-plugin",
    module: "ClustersModule",
    to: clusterId,
    description:
      (
        (resource.resource as Record<string, unknown>).state as string
      )?.toLowerCase() ?? "unknown",
  };
}

export { default as KindClusterIcon } from "./KindIcon";
