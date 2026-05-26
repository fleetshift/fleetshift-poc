/**
 * API helpers for the FleetShift management plane.
 * Requests go through the Express proxy:
 *   /api/v1/management/* → Go backend at :8085 /v1/*
 */

import type {
  MgmtDeployment,
  ListDeploymentsResponse,
  CreateDeploymentRequest,
  AuthMethod,
  CreateAuthMethodRequest,
} from "../types/management";

const BASE = "/api/v1/management";

async function mgmtFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, init);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const msg =
      (body as Record<string, string>).message ||
      (body as Record<string, string>).error ||
      `Management API error (${res.status})`;
    throw new Error(msg);
  }
  return res.json();
}

// --- Deployments ---

export function listDeployments(
  pageSize?: number,
  pageToken?: string,
): Promise<ListDeploymentsResponse> {
  const params = new URLSearchParams();
  if (pageSize) params.set("page_size", String(pageSize));
  if (pageToken) params.set("page_token", pageToken);
  const qs = params.toString();
  return mgmtFetch(`/deployments${qs ? `?${qs}` : ""}`);
}

export function getDeployment(name: string): Promise<MgmtDeployment> {
  return mgmtFetch(`/deployments/${encodeURIComponent(name)}`);
}

export function createDeployment(
  req: CreateDeploymentRequest,
): Promise<MgmtDeployment> {
  return mgmtFetch("/deployments", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req),
  });
}

export function deleteDeployment(name: string): Promise<MgmtDeployment> {
  return mgmtFetch(`/deployments/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });
}

export function resumeDeployment(name: string): Promise<MgmtDeployment> {
  return mgmtFetch(`/deployments/${encodeURIComponent(name)}:resume`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({}),
  });
}

// --- Auth Methods ---

export function getAuthMethod(name: string): Promise<AuthMethod> {
  return mgmtFetch(`/authMethods/${encodeURIComponent(name)}`);
}

export function createAuthMethod(
  req: CreateAuthMethodRequest,
): Promise<AuthMethod> {
  return mgmtFetch("/authMethods", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req),
  });
}
