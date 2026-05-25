const MGMT_BASE = "/v1";

export async function mgmtFetch<T>(
  path: string,
  init?: RequestInit,
): Promise<T> {
  const res = await fetch(`${MGMT_BASE}${path}`, init);
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

export interface RegistrySubjectMapping {
  registryId: string;
  expression: string;
}

export interface OIDCConfig {
  issuerUrl: string;
  audience: string;
  keyEnrollmentAudience?: string;
  authorizationEndpoint?: string;
  tokenEndpoint?: string;
  jwksUri?: string;
  registrySubjectMapping?: RegistrySubjectMapping;
}

export interface AuthMethod {
  name: string;
  type: "TYPE_UNSPECIFIED" | "TYPE_OIDC";
  oidcConfig?: OIDCConfig;
}

export interface CreateAuthMethodRequest {
  authMethodId: string;
  authMethod: {
    type: "TYPE_OIDC";
    oidcConfig: {
      issuerUrl: string;
      audience: string;
      keyEnrollmentAudience?: string;
    };
  };
}

export function createAuthMethod(
  req: CreateAuthMethodRequest,
): Promise<AuthMethod> {
  const params = new URLSearchParams();
  params.set("auth_method_id", req.authMethodId);
  return mgmtFetch(`/authMethods?${params}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req.authMethod),
  });
}

export function getAuthMethod(name: string): Promise<AuthMethod> {
  return mgmtFetch(`/authMethods/${encodeURIComponent(name)}`);
}

// --- Deployments ---

export type DeploymentState =
  | "STATE_UNSPECIFIED"
  | "STATE_CREATING"
  | "STATE_ACTIVE"
  | "STATE_DELETING"
  | "STATE_FAILED"
  | "STATE_PAUSED_AUTH";

export interface Manifest {
  resourceType: string;
  raw: string;
}

export interface ManifestStrategy {
  type: "TYPE_UNSPECIFIED" | "TYPE_INLINE";
  manifests?: Manifest[];
}

export interface TargetSelector {
  matchLabels: Record<string, string>;
}

export interface PlacementStrategy {
  type: "TYPE_UNSPECIFIED" | "TYPE_STATIC" | "TYPE_ALL" | "TYPE_SELECTOR";
  targetIds?: string[];
  targetSelector?: TargetSelector;
}

export interface RolloutStrategy {
  type: "TYPE_UNSPECIFIED" | "TYPE_IMMEDIATE";
}

export interface MgmtDeployment {
  name: string;
  uid: string;
  manifestStrategy: ManifestStrategy;
  placementStrategy: PlacementStrategy;
  rolloutStrategy?: RolloutStrategy;
  resolvedTargetIds: string[];
  state: DeploymentState;
  reconciling: boolean;
  createTime: string;
  updateTime: string;
  etag: string;
}

export interface ListDeploymentsResponse {
  deployments: MgmtDeployment[];
  nextPageToken: string;
}

export function listDeployments(): Promise<ListDeploymentsResponse> {
  return mgmtFetch("/deployments");
}

export function getDeployment(name: string): Promise<MgmtDeployment> {
  return mgmtFetch(`/deployments/${encodeURIComponent(name)}`);
}

export interface CreateDeploymentRequest {
  deploymentId: string;
  deployment: {
    manifestStrategy: ManifestStrategy;
    placementStrategy: PlacementStrategy;
    rolloutStrategy?: RolloutStrategy;
  };
  /** Base64-encoded ECDSA-P256-SHA256 ASN.1 DER signature. */
  userSignature?: string;
  /** ISO 8601 timestamp for signing envelope reconstruction. */
  validUntil?: string;
  expectedGeneration?: number;
}

export function createDeployment(
  req: CreateDeploymentRequest,
): Promise<MgmtDeployment> {
  const params = new URLSearchParams();
  params.set("deployment_id", req.deploymentId);
  if (req.validUntil) {
    params.set("valid_until", req.validUntil);
  }
  if (req.expectedGeneration) {
    params.set("expected_generation", String(req.expectedGeneration));
  }
  if (req.userSignature) {
    params.set("user_signature", req.userSignature);
  }
  return mgmtFetch(`/deployments?${params}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req.deployment),
  });
}

// --- Signer Enrollments ---

export interface SignerEnrollment {
  name: string;
  subject: string;
  issuer: string;
  registrySubject: string;
  registryId: string;
  createTime: string;
  expireTime: string;
}

export interface CreateSignerEnrollmentRequest {
  signerEnrollmentId: string;
  identityToken: string;
  registryId?: string;
}

export function createSignerEnrollment(
  req: CreateSignerEnrollmentRequest,
): Promise<SignerEnrollment> {
  return mgmtFetch("/signerEnrollments", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      signer_enrollment_id: req.signerEnrollmentId,
      identity_token: req.identityToken,
      ...(req.registryId && { registry_id: req.registryId }),
    }),
  });
}

export function deleteSignerEnrollment(): Promise<void> {
  return mgmtFetch("/signerEnrollments", { method: "DELETE" });
}

export function deleteDeployment(name: string): Promise<MgmtDeployment> {
  return mgmtFetch(`/deployments/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });
}

export interface ResumeDeploymentRequest {
  name: string;
  /** Base64-encoded ECDSA-P256-SHA256 ASN.1 DER signature for re-signing. */
  userSignature?: string;
  validUntil?: string;
}

export function resumeDeployment(
  req: ResumeDeploymentRequest,
): Promise<MgmtDeployment> {
  const body: Record<string, unknown> = {};
  if (req.validUntil) {
    body.valid_until = req.validUntil;
  }
  if (req.userSignature) {
    body.user_signature = req.userSignature;
  }
  return mgmtFetch(`/deployments/${encodeURIComponent(req.name)}:resume`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}
