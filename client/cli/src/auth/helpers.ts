import { readFileSync } from "node:fs";

import { Agent } from "undici";

import type { StoredTokens } from "../config";
import { saveStoredTokens } from "../config";

export interface TokenResponse {
  access_token: string;
  refresh_token?: string;
  id_token?: string;
  token_type?: string;
  expires_in?: number;
}

export async function oidcRequest<T>(
  url: string,
  init: RequestInit,
  caFile?: string,
): Promise<T> {
  const dispatcher = caFile
    ? new Agent({ connect: { ca: readFileSync(caFile) } })
    : undefined;
  const response = await fetch(url, {
    ...init,
    ...(dispatcher ? { dispatcher } : {}),
  } as RequestInit);
  if (!response.ok) {
    throw new Error(
      `OIDC request failed: ${response.status} ${response.statusText}: ${await response.text()}`,
    );
  }
  return response.json() as Promise<T>;
}

export async function discoverOidc(
  issuer: string,
  caFile?: string,
): Promise<{ authorization_endpoint: string; token_endpoint: string }> {
  const dispatcher = caFile
    ? new Agent({ connect: { ca: readFileSync(caFile) } })
    : undefined;
  const response = await fetch(
    `${issuer.replace(/\/$/, "")}/.well-known/openid-configuration`,
    dispatcher ? ({ dispatcher } as RequestInit) : undefined,
  );
  if (!response.ok)
    throw new Error(
      `OIDC discovery failed: ${response.status} ${response.statusText}`,
    );
  const data = (await response.json()) as {
    authorization_endpoint?: string;
    token_endpoint?: string;
  };
  if (!data.authorization_endpoint || !data.token_endpoint)
    throw new Error("OIDC discovery response missing required endpoints");
  return {
    authorization_endpoint: data.authorization_endpoint,
    token_endpoint: data.token_endpoint,
  };
}

export async function saveTokenResponse(
  directory: string | undefined,
  token: TokenResponse,
): Promise<void> {
  const tokens: StoredTokens = {
    access_token: token.access_token,
    ...(token.refresh_token ? { refresh_token: token.refresh_token } : {}),
    ...(token.id_token ? { id_token: token.id_token } : {}),
    expiry: new Date(
      Date.now() + (token.expires_in ?? 3600) * 1000,
    ).toISOString(),
    token_type: token.token_type ?? "Bearer",
  };
  await saveStoredTokens(directory, tokens);
}
