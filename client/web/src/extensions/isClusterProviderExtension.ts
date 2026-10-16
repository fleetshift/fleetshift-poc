import type {
  ClusterProviderCardProps,
  ClusterProviderWizardProps,
} from "@ocm-resource-manager/common";
import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import type { ComponentType } from "react";

import type { SearchReservedProperties } from "./searchTypes";

export type ClusterProviderExtension = Extension<
  "ocm-resource-manager.cluster-provider",
  {
    id: string;
    label: string;
    description: string;
    keywords?: string[];
    to?: { pathname?: string; search?: string; hash?: string };
    icon: CodeRef<ComponentType>;
    card: CodeRef<ComponentType<ClusterProviderCardProps>>;
    wizard: CodeRef<ComponentType<ClusterProviderWizardProps>>;
  } & SearchReservedProperties
>;

export function isClusterProviderExtension(
  e: Extension,
): e is ClusterProviderExtension {
  return e.type === "ocm-resource-manager.cluster-provider";
}
