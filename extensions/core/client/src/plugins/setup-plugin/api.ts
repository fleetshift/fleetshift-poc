export interface OidcConfig {
  issuerUrl: string;
  audience: string;
  authorizationEndpoint: string;
  tokenEndpoint: string;
  jwksUri: string;
  registrySubjectMapping?: {
    registryId: string;
    expression: string;
  };
}

export interface AuthMethod {
  name: string;
  type: string;
  oidcConfig: OidcConfig;
}

export type AuthState =
  | { status: "idle" }
  | { status: "submitting" }
  | { status: "configured"; authMethod: AuthMethod }
  | { status: "error"; message: string };

export async function fetchAuthMethod(): Promise<AuthMethod | null> {
  const res = await fetch("/v1/authMethods/default");
  if (res.status === 404 || res.status === 500) return null;
  if (!res.ok) throw new Error(`Unexpected status ${res.status}`);
  return res.json();
}

async function getOidcClientId(): Promise<string> {
  const res = await fetch("/api/ui/config");
  if (!res.ok) {
    throw new Error(`Failed to fetch UI config (${res.status})`);
  }
  const config = await res.json();
  return config.oidc?.clientId ?? "fleetshift-ui";
}

export async function triggerAuthSetup(
  issuerUrl: string,
  audience: string,
  keyRegistry: "oidc" | "github",
): Promise<void> {
  const enrollmentAudience = await getOidcClientId();
  const oidcConfig: Record<string, unknown> = {
    issuer_url: issuerUrl.replace(/\/+$/, ""),
    audience,
    key_enrollment_audience: enrollmentAudience,
  };

  if (keyRegistry === "github") {
    oidcConfig.registry_subject_mapping = {
      registry_id: "github.com",
      expression: "claims.github_username",
    };
  } else {
    oidcConfig.public_key_claim_expression = "claims.signing_public_key";
  }

  const res = await fetch("/v1/authMethods?auth_method_id=default", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ type: "TYPE_OIDC", oidc_config: oidcConfig }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Auth setup failed (${res.status}): ${text}`);
  }
}
