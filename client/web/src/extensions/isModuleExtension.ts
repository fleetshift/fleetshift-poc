import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import type { ComponentType } from "react";

import type { SearchReservedProperties } from "./searchTypes";

export type ExtensionPointDeclaration = {
  description: string;
  type: string;
};

export type ModuleExtension = Extension<
  "fleetshift.module",
  {
    id: string;
    label: string;
    component: CodeRef<ComponentType>;
    icon: CodeRef<ComponentType>;
    description?: string;
    keywords?: string[];
    extensionPoints?: Record<string, ExtensionPointDeclaration>;
  } & SearchReservedProperties
>;

export function isModuleExtension(e: Extension): e is ModuleExtension {
  return e.type === "fleetshift.module";
}
