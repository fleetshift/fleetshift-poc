import type { ClusterDetailTabProps } from "@ocm-fleet/common";
import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import type { ComponentType } from "react";

export type ClusterDetailTabExtension = Extension<
  "ocm-fleet.cluster-detail-tab",
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
  return e.type === "ocm-fleet.cluster-detail-tab";
}
