import type { ClusterDetailTabProps } from "@fleetshift/common";
import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import type { ComponentType } from "react";

export type ClusterDetailTabExtension = Extension<
  "fleetshift.cluster-detail-tab",
  {
    id: string;
    label: string;
    title: string;
    eventKey: string;
    priority?: number;
    component: CodeRef<ComponentType<ClusterDetailTabProps>>;
  }
>;

export function isClusterDetailTabExtension(
  e: Extension,
): e is ClusterDetailTabExtension {
  return e.type === "fleetshift.cluster-detail-tab";
}
