import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import type { ComponentType } from "react";
import type {
  ClusterProviderCardProps,
  ClusterProviderWizardProps,
} from "@fleetshift/common";

export type ClusterProviderExtension = Extension<
  "fleetshift.cluster-provider",
  {
    id: string;
    label: string;
    description: string;
    icon: CodeRef<ComponentType>;
    card: CodeRef<ComponentType<ClusterProviderCardProps>>;
    wizard: CodeRef<ComponentType<ClusterProviderWizardProps>>;
  }
>;

export function isClusterProviderExtension(
  e: Extension,
): e is ClusterProviderExtension {
  return e.type === "fleetshift.cluster-provider";
}
