import type { SearchResultResolve } from "@ocm-resource-manager/common";
import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import type { ComponentType } from "react";

export type SearchResultRendererExtension = Extension<
  "ocm-resource-manager.render-search",
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
  return e.type === "ocm-resource-manager.render-search";
}
