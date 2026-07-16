import type { SearchResultResolve } from "@fleetshift/common";
import { createResourceApi } from "@fleetshift/common";
import { useResolvedExtensions } from "@openshift/dynamic-plugin-sdk";
import { Label } from "@patternfly/react-core";
import {
  type ComponentType,
  type ReactNode,
  useCallback,
  useMemo,
} from "react";

import { highlightText } from "../components/Search/highlightUtils";
import type { SearchResultItem } from "../components/Search/searchIndex";
import type { SearchResultRendererExtension } from "../extensions/isSearchResultRendererExtension";
import { isSearchResultRendererExtension } from "../extensions/isSearchResultRendererExtension";

interface ResolvedRenderer {
  label: string;
  resolve: SearchResultResolve;
  icon?: ComponentType;
}

function badgedDescription(badge: string, description: ReactNode): ReactNode {
  return (
    <>
      <Label isCompact color="blue">
        {badge}
      </Label>{" "}
      {description}
    </>
  );
}

const client = createResourceApi("-");

export function useInventorySearch(): {
  search: (term: string) => Promise<SearchResultItem[]>;
  loaded: boolean;
} {
  const [extensions, loaded] =
    useResolvedExtensions<SearchResultRendererExtension>(
      isSearchResultRendererExtension,
    );

  const rendererMap = useMemo(() => {
    const map = new Map<string, ResolvedRenderer>();
    for (const ext of extensions) {
      map.set(ext.properties.resourceType, {
        label: ext.properties.label,
        resolve: ext.properties.resolve,
        icon: ext.properties.icon,
      });
    }
    return map;
  }, [extensions]);

  const search = useCallback(
    async (term: string): Promise<SearchResultItem[]> => {
      try {
        const escaped = term.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
        const clusterFilter = [
          `(resourceType == "gcphcp.fleetshift.io/Cluster" || resourceType == "kind.fleetshift.io/Cluster")`,
          `&& (resource.name.startsWith("${escaped}") || resource.name.startsWith("clusters/${escaped}"))`,
        ].join(" ");
        const k8sFilter = [
          `resourceType == "kubernetes.fleetshift.io/Object"`,
          `&& (resource.observation.kind == "Pod" || resource.observation.kind == "Namespace" || resource.observation.kind == "Node")`,
          `&& resource.observation.metadata.name.startsWith("${escaped}")`,
        ].join(" ");
        const [clusterResponse, k8sResponse] = await Promise.all([
          client.search({ filter: clusterFilter, pageSize: 5 }),
          client.search({ filter: k8sFilter, pageSize: 5 }),
        ]);
        const allResults = [
          ...clusterResponse.resources,
          ...k8sResponse.resources,
        ];

        return allResults.map((result) => {
          const renderer = rendererMap.get(result.resourceType);

          const id =
            ((result.resource as Record<string, unknown>).uuid as string) ??
            ((result.resource as Record<string, unknown>).uid as string) ??
            result.resource.name;
          if (!renderer) {
            return {
              id,
              title: highlightText(
                term,
                result.resource.name.split("/").pop() ?? result.resource.name,
              ),
              description: result.resourceType,
              category: "resources",
              pathname: "",
              icon: "",
            };
          }

          let rendered;
          try {
            rendered = renderer.resolve(result);
          } catch (err) {
            console.error(err);
            return {
              id,
              title: highlightText(
                term,
                result.resource.name.split("/").pop() ?? result.resource.name,
              ),
              description: result.resourceType,
              category: "resources",
              pathname: "",
              icon: "",
            };
          }

          return {
            id,
            title: highlightText(
              term,
              rendered.title ??
                result.resource.name.split("/").pop() ??
                result.resource.name,
            ),
            description: "",
            descriptionNode: badgedDescription(
              renderer.label,
              rendered.description,
            ),
            category: "resources",
            pathname: "",
            icon: "",
            IconComponent: renderer.icon,
            pluginLink: {
              scope: rendered.scope,
              module: rendered.module,
              to: rendered.to,
              search: rendered.search,
            },
          };
        });
      } catch (error) {
        console.error(error);
        return [];
      }
    },
    [rendererMap],
  );

  return { search, loaded };
}
