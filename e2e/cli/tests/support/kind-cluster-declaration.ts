import { type TestDetails, type TestDetailsAnnotation } from "@playwright/test";

import {
  type KindClusterAccess,
  type KindClusterRequest,
  type KindClusterStateRequirement,
} from "./kind-pool";
import { parseKindClusterCreateSpec } from "./kind-spec";

export const KIND_CLUSTERS_ANNOTATION_TYPE = "fleetshift.kindClusters";

export type KindClusterTestDetails = TestDetails & {
  kindClusters: readonly KindClusterRequest[];
};

export function encodeKindClusterDetails(
  details: KindClusterTestDetails,
): TestDetails {
  const annotation: TestDetailsAnnotation = {
    description: JSON.stringify(details.kindClusters),
    type: KIND_CLUSTERS_ANNOTATION_TYPE,
  };
  const existing = existingAnnotations(details.annotation);
  const { kindClusters: _kindClusters, ...rest } = details;
  return {
    ...rest,
    annotation: [...existing, annotation],
  };
}

export function readKindClusterRequests(
  annotations: readonly TestDetailsAnnotation[],
): KindClusterRequest[] {
  const matches = annotations.filter(
    (annotation) => annotation.type === KIND_CLUSTERS_ANNOTATION_TYPE,
  );
  if (matches.length === 0) {
    throw new Error("test is missing a kindClusters declaration");
  }
  if (matches.length > 1) {
    throw new Error("test has a duplicated kindClusters declaration");
  }
  return parseKindClusterRequests(matches[0]?.description);
}

function existingAnnotations(
  annotation: TestDetails["annotation"],
): TestDetailsAnnotation[] {
  if (annotation === undefined) return [];
  return Array.isArray(annotation) ? [...annotation] : [annotation];
}

function parseKindClusterRequests(
  raw: string | undefined,
): KindClusterRequest[] {
  if (raw === undefined || raw.trim() === "") {
    throw new Error("kindClusters declaration is malformed");
  }
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    throw new Error("kindClusters declaration is malformed");
  }
  if (!Array.isArray(value)) {
    throw new Error("kindClusters declaration is malformed");
  }
  return value.map(parseKindClusterRequest);
}

function parseKindClusterRequest(value: unknown): KindClusterRequest {
  if (typeof value !== "object" || value === null) {
    throw new Error("kindClusters declaration is malformed");
  }
  const request = value as {
    access?: unknown;
    spec?: unknown;
    state?: unknown;
  };
  if (!isAccess(request.access) || !isState(request.state)) {
    throw new Error("kindClusters declaration is malformed");
  }
  if (
    !("spec" in request) ||
    request.spec === undefined ||
    request.spec === null
  ) {
    throw new Error("kindClusters declaration is malformed");
  }
  if (request.spec === "any") {
    return { access: request.access, spec: "any", state: request.state };
  }
  try {
    return {
      access: request.access,
      spec: parseKindClusterCreateSpec(request.spec),
      state: request.state,
    };
  } catch {
    throw new Error("kindClusters declaration is malformed");
  }
}

function isAccess(value: unknown): value is KindClusterAccess {
  return value === "read-only" || value === "modifiable";
}

function isState(value: unknown): value is KindClusterStateRequirement {
  return value === "clean" || value === "any";
}
