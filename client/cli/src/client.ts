import { readFileSync } from "node:fs";

import { Agent } from "undici";

export interface CliClient {
  request<T>(path: string, init?: RequestInit): Promise<T>;
}

export function createCliClient(
  server: string,
  token?: string,
  caFile?: string,
): CliClient {
  const base = server.includes("://") ? server : `https://${server}`;
  const dispatcher = caFile
    ? new Agent({ connect: { ca: readFileSync(caFile) } })
    : undefined;
  return {
    async request<T>(path: string, init: RequestInit = {}) {
      const headers = new Headers(init.headers);
      headers.set("Accept", "application/json");
      if (init.body && !headers.has("Content-Type"))
        headers.set("Content-Type", "application/json");
      if (token) headers.set("Authorization", `Bearer ${token}`);
      const response = await fetch(new URL(path, `${base}/`), {
        ...init,
        headers,
        ...(dispatcher ? { dispatcher } : {}),
      } as RequestInit);
      if (!response.ok) {
        const body = await response.text();
        throw new Error(
          `${response.status} ${response.statusText}${body ? `: ${body}` : ""}`,
        );
      }
      if (response.status === 204) return undefined as T;
      return response.json() as Promise<T>;
    },
  };
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
