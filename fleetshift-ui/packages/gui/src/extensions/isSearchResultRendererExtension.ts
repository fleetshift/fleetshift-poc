import type { SearchResultResolve } from "@fleetshift/common";
import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import type { ComponentType } from "react";

export type SearchResultRendererExtension = Extension<
  "fleetshift.render-search",
  {
    id: string;
    label: string;
    resourceType: string;
    resolve: CodeRef<SearchResultResolve>;
    component?: CodeRef<ComponentType>;
    icon?: CodeRef<ComponentType>;
  }
>;

export function isSearchResultRendererExtension(
  e: Extension,
): e is SearchResultRendererExtension {
  return e.type === "fleetshift.render-search";
}
