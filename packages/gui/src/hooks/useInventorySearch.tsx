import type { ResourceResult, SearchResultResolve } from "@fleetshift/common";
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
  filterSearch: (celFilter: string) => Promise<SearchResultItem[]>;
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

  const mapResult = useCallback(
    (result: ResourceResult, highlight?: string): SearchResultItem => {
      const renderer = rendererMap.get(result.resourceType);
      const id =
        ((result.resource as Record<string, unknown>).uuid as string) ??
        ((result.resource as Record<string, unknown>).uid as string) ??
        result.resource.name;
      const rawName =
        result.resource.name.split("/").pop() ?? result.resource.name;

      if (!renderer) {
        return {
          id,
          title: highlight ? highlightText(highlight, rawName) : rawName,
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
          title: highlight ? highlightText(highlight, rawName) : rawName,
          description: result.resourceType,
          category: "resources",
          pathname: "",
          icon: "",
        };
      }

      const title = rendered.title ?? rawName;
      return {
        id,
        title: highlight ? highlightText(highlight, title) : title,
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
    },
    [rendererMap],
  );

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
        return allResults.map((r) => mapResult(r, term));
      } catch (error) {
        console.error(error);
        return [];
      }
    },
    [mapResult],
  );

  const filterSearch = useCallback(
    async (celFilter: string): Promise<SearchResultItem[]> => {
      console.info("[filterSearch] CEL filter:", celFilter);
      try {
        const response = await client.search({
          filter: celFilter,
          pageSize: 20,
        });
        console.info("[filterSearch] results:", response.resources.length);
        return response.resources.map((r) => mapResult(r));
      } catch (error) {
        console.error("[filterSearch] error:", error);
        return [];
      }
    },
    [mapResult],
  );

  return { search, filterSearch, loaded };
}
