const MGMT_BASE = "/v1";

async function mgmtFetch<T>(path: string, init?: RequestInit): Promise<T> {
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

export function getAuthMethod(name: string): Promise<AuthMethod> {
  return mgmtFetch(`/authMethods/${encodeURIComponent(name)}`);
}

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
