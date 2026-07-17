import type {
  BaseExtensionProperties,
  EncodedCodeRef,
  FleetshiftExtension,
} from "./types";
import { validateCodeRef } from "./validate";

export const CLUSTER_DETAIL_TAB_TYPE = "fleetshift.cluster-detail-tab" as const;

export type ClusterDetailTabExtras = {
  component: EncodedCodeRef;
  title: string;
  eventKey: string;
  priority?: number;
};

export type ClusterDetailTabProperties = BaseExtensionProperties &
  ClusterDetailTabExtras;

function validateId(id: unknown, context: string): string[] {
  if (typeof id !== "string" || id.length === 0) {
    return [`${context}: "id" is required and must be a non-empty string`];
  }
  if (!/^[a-z][a-z0-9-]*$/.test(id)) {
    return [
      `${context}: "id" must start with a lowercase letter and contain only a-z, 0-9, hyphens (got "${id}")`,
    ];
  }
  return [];
}

function validateLabel(label: unknown, context: string): string[] {
  if (typeof label !== "string" || label.length === 0) {
    return [`${context}: "label" is required and must be a non-empty string`];
  }
  return [];
}

export function validateClusterDetailTabProperties(
  props: ClusterDetailTabProperties,
  ctx: string,
): string[] {
  const errors = [
    ...validateId(props.id, ctx),
    ...validateLabel(props.label, ctx),
    ...validateCodeRef(props.component, "component", ctx),
  ];
  if (typeof props.title !== "string" || props.title.length === 0) {
    errors.push(`${ctx}: "title" is required and must be a non-empty string`);
  }
  if (typeof props.eventKey !== "string" || props.eventKey.length === 0) {
    errors.push(
      `${ctx}: "eventKey" is required and must be a non-empty string`,
    );
  }
  if (props.priority !== undefined && typeof props.priority !== "number") {
    errors.push(`${ctx}: "priority" must be a number when provided`);
  }
  return errors;
}

function throwOnErrors(errors: string[], type: string): void {
  if (errors.length > 0) {
    throw new Error(
      `Invalid ${type} extension:\n${errors.map((e) => `  - ${e}`).join("\n")}`,
    );
  }
}

export function createClusterDetailTab(
  properties: ClusterDetailTabProperties,
): FleetshiftExtension<typeof CLUSTER_DETAIL_TAB_TYPE, ClusterDetailTabExtras> {
  const ctx = `${CLUSTER_DETAIL_TAB_TYPE} "${properties.id || "(no id)"}"`;
  throwOnErrors(
    validateClusterDetailTabProperties(properties, ctx),
    CLUSTER_DETAIL_TAB_TYPE,
  );
  return { type: CLUSTER_DETAIL_TAB_TYPE, properties };
}
