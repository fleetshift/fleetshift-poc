import {
  authMethodServiceCreateAuthMethod,
  authMethodServiceGetAuthMethod,
  getUiConfig,
} from "@fleetshift/common";
import { client } from "@fleetshift/common/dynamic/client/generated/client.gen";
import type { V1OidcConfigWritable } from "@fleetshift/common/dynamic/client/generated/types.gen";

client.setConfig({ baseUrl: window.location.origin });

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
  const result = await authMethodServiceGetAuthMethod({
    client,
    path: { name: "authMethods/default" },
  });
  if (result.error) {
    const status = result.response?.status;
    if (status === 404 || status === 500) return null;
    throw result.error;
  }
  if (!result.data) return null;
  const oidc = result.data.oidcConfig;
  if (!oidc) throw new Error("Auth method response missing OIDC config");
  return {
    name: result.data.name ?? "authMethods/default",
    type: result.data.type ?? "TYPE_UNSPECIFIED",
    oidcConfig: {
      issuerUrl: oidc.issuerUrl ?? "",
      audience: oidc.audience ?? "",
      authorizationEndpoint: oidc.authorizationEndpoint ?? "",
      tokenEndpoint: oidc.tokenEndpoint ?? "",
      jwksUri: oidc.jwksUri ?? "",
      registrySubjectMapping: oidc.registrySubjectMapping
        ? {
            registryId: oidc.registrySubjectMapping.registryId ?? "",
            expression: oidc.registrySubjectMapping.expression ?? "",
          }
        : undefined,
    },
  };
}

async function getOidcClientId(): Promise<string> {
  const result = await getUiConfig({ client });
  if (result.error) throw result.error;
  if (!result.data) throw new Error("UI config response missing data");
  return result.data.oidc.clientId;
}

export async function triggerAuthSetup(
  issuerUrl: string,
  audience: string,
  keyRegistry: "oidc" | "github",
): Promise<void> {
  const enrollmentAudience = await getOidcClientId();
  const oidcConfig: V1OidcConfigWritable = {
    issuerUrl: issuerUrl.replace(/\/+$/, ""),
    audience,
    keyEnrollmentAudience: enrollmentAudience,
    ...(keyRegistry === "github"
      ? {
          registrySubjectMapping: {
            registryId: "github.com",
            expression: "claims.github_username",
          },
        }
      : { publicKeyClaimExpression: "claims.signing_public_key" }),
  };

  const result = await authMethodServiceCreateAuthMethod({
    client,
    query: { authMethodId: "default" },
    body: { type: "TYPE_OIDC", oidcConfig },
  });
  if (result.error) throw result.error;
}
