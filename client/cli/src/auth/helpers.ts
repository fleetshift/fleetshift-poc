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
