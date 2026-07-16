import { createResourceApi, type ResourceResult } from "@fleetshift/common";
import { useCallback, useEffect, useRef, useState } from "react";

export interface K8sObservationMetadata {
  uid: string;
  namespace: string;
  name: string;
  resourceVersion: string;
  generation: number;
  creationTimestamp: string;
  deletionTimestamp: string | null;
  annotations: Record<string, string> | null;
  ownerReferences: unknown[] | null;
}

export interface K8sObservation {
  gvr: {
    group: string;
    version: string;
    resource: string;
    scope: string;
  };
  apiVersion: string;
  kind: string;
  metadata: K8sObservationMetadata;
  extracted: Record<string, unknown>;
}

export interface K8sObjectResource {
  observation: K8sObservation;
}

const k8sApi = createResourceApi<K8sObjectResource>("-");

const GVR_TO_KIND: Record<string, string> = {
  "core~v1~pods": "Pod",
  "core~v1~namespaces": "Namespace",
  "core~v1~nodes": "Node",
};

export function useClusterResources(
  clusterId: string | undefined,
  gvrKey: string,
): {
  resources: ResourceResult<K8sObjectResource>[];
  loading: boolean;
  error: string | null;
  refetch: () => void;
} {
  const [resources, setResources] = useState<
    ResourceResult<K8sObjectResource>[]
  >([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const requestIdRef = useRef(0);

  const fetch = useCallback(async () => {
    const id = ++requestIdRef.current;
    setLoading(true);
    setError(null);
    try {
      const kind = GVR_TO_KIND[gvrKey];
      const parts = [`resourceType == "kubernetes.fleetshift.io/Object"`];
      if (clusterId) {
        const escaped = clusterId.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
        parts.push(
          `name.startsWith("//kubernetes.fleetshift.io/clusters/${escaped}/apiResources/${gvrKey}/")`,
        );
      } else if (kind) {
        parts.push(`resource.observation.kind == "${kind}"`);
      }
      const filter = parts.join(" && ");
      const { resources: results } = await k8sApi.search({
        filter,
        pageSize: 200,
      });
      if (id !== requestIdRef.current) return;
      setResources(results);
    } catch (e) {
      if (id !== requestIdRef.current) return;
      setError(e instanceof Error ? e.message : "Failed to load resources");
    } finally {
      if (id === requestIdRef.current) setLoading(false);
    }
  }, [clusterId, gvrKey]);

  useEffect(() => {
    fetch();
  }, [fetch]);

  return { resources, loading, error, refetch: fetch };
}

const CANONICAL_RE =
  /\/\/kubernetes\.fleetshift\.io\/clusters\/([^/]+)\/apiResources\/[^/]+\/objects\/[^/]+/;

export function extractClusterId(resourceName: string): string {
  const m = CANONICAL_RE.exec(resourceName);
  return m?.[1] ?? "—";
}

export function formatAge(iso: string | undefined): string {
  if (!iso) return "—";
  const created = new Date(iso);
  const now = new Date();
  const diffMs = now.getTime() - created.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  if (diffMins < 1) return "<1m";
  if (diffMins < 60) return `${diffMins}m`;
  const diffHours = Math.floor(diffMins / 60);
  if (diffHours < 24) return `${diffHours}h`;
  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays}d`;
}
