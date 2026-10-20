import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import type { ComponentType } from "react";

import type { SearchReservedProperties } from "./searchTypes";

export type ExtensionPointDeclaration = {
  description: string;
  type: string;
};

export type RouteExtension = Extension<
  "ocm-fleet.route",
  {
    id: string;
    label: string;
    component: CodeRef<ComponentType>;
    icon: CodeRef<ComponentType>;
    group?: string;
    description?: string;
    keywords?: string[];
    extensionPoints?: Record<string, ExtensionPointDeclaration>;
  } & SearchReservedProperties
>;

export function isRouteExtension(e: Extension): e is RouteExtension {
  return e.type === "ocm-fleet.route";
}
