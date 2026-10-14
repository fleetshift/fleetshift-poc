import type {
  BaseExtensionProperties,
  EncodedCodeRef,
  FleetshiftExtension,
} from "./types";
import { validateCodeRef } from "./validate";

export const RENDER_SEARCH_TYPE = "fleetshift.render-search" as const;

export type SearchResultRendererExtras = {
  resourceType: string;
  resolve: EncodedCodeRef;
  component?: EncodedCodeRef;
  icon?: EncodedCodeRef;
};

export type SearchResultRendererProperties = BaseExtensionProperties &
  SearchResultRendererExtras;

export type SearchResultRendererInput = SearchResultRendererProperties;

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

export function validateSearchResultRendererProperties(
  props: SearchResultRendererProperties,
  ctx: string,
): string[] {
  const errors = [
    ...validateId(props.id, ctx),
    ...validateLabel(props.label, ctx),
    ...validateCodeRef(props.resolve, "resolve", ctx),
    ...(props.component
      ? validateCodeRef(props.component, "component", ctx)
      : []),
    ...(props.icon ? validateCodeRef(props.icon, "icon", ctx) : []),
  ];
  if (
    typeof props.resourceType !== "string" ||
    props.resourceType.length === 0
  ) {
    errors.push(
      `${ctx}: "resourceType" is required and must be a non-empty string`,
    );
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

export function createSearchResultRenderer(
  properties: SearchResultRendererInput,
): FleetshiftExtension<typeof RENDER_SEARCH_TYPE, SearchResultRendererExtras> {
  const ctx = `${RENDER_SEARCH_TYPE} "${properties.id || "(no id)"}"`;
  throwOnErrors(
    validateSearchResultRendererProperties(properties, ctx),
    RENDER_SEARCH_TYPE,
  );
  return { type: RENDER_SEARCH_TYPE, properties };
}
