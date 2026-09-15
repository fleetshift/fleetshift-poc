import { readFileSync } from "node:fs";

import { client } from "@fleetshift/common/dynamic/client/generated/client.gen";
import { Agent } from "undici";

import { flagString } from "../argv";
import { oidcRequest, type TokenResponse } from "../auth/helpers";
import { loadAuthConfig, loadStoredTokens, saveStoredTokens } from "../config";

export async function unwrap<T>(
  result: Promise<{
    data?: T;
    error?: unknown;
    response?: Response;
  }>,
): Promise<T> {
  const resultValue = await result;
  if (resultValue.error) {
    const error = resultValue.error;
    const message =
      typeof error === "object" && error !== null && "message" in error
        ? String(error.message)
        : String(error);
    const status = resultValue.response?.status;
    throw new Error(`${status ? `${status} ` : ""}${message}`, {
      cause: error,
    });
  }
  if (resultValue.data === undefined)
    throw new Error("No data returned from API");
  return resultValue.data;
}

function configureCliClient(
  server: string,
  token?: string,
  caFile?: string,
): typeof client {
  const baseUrl = server.includes("://") ? server : `https://${server}`;
  const dispatcher = caFile
    ? new Agent({ connect: { ca: readFileSync(caFile) } })
    : undefined;
  client.setConfig({
    baseUrl,
    responseStyle: "fields",
    throwOnError: false,
    fetch: (input, init) => {
      const headers = new Headers(init?.headers);
      headers.set("Accept", "application/json");
      if (init?.body && !headers.has("Content-Type")) {
        headers.set("Content-Type", "application/json");
      }
      if (token) headers.set("Authorization", `Bearer ${token}`);
      return fetch(input, {
        ...init,
        headers,
        ...(dispatcher ? { dispatcher } : {}),
      } as RequestInit);
    },
  });
  return client;
}

export async function clientForArgs(
  args: Parameters<typeof flagString>[0],
  accessToken?: string,
): Promise<ReturnType<typeof configureCliClient>> {
  const configDirectory = flagString(args, "config-dir") || undefined;
  const authConfig = await loadAuthConfig(configDirectory).catch(
    () => undefined,
  );
  let tokens = await loadStoredTokens(configDirectory).catch(() => undefined);
  if (
    authConfig &&
    tokens?.refresh_token &&
    new Date(tokens.expiry).getTime() - Date.now() <= 30_000
  ) {
    const refreshed = await oidcRequest<TokenResponse>(
      authConfig.token_endpoint,
      {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          client_id: authConfig.client_id,
          grant_type: "refresh_token",
          refresh_token: tokens.refresh_token,
        }),
      },
      authConfig.oidc_ca_file,
    );
    await saveStoredTokens(configDirectory, {
      access_token: refreshed.access_token,
      refresh_token: refreshed.refresh_token ?? tokens.refresh_token,
      ...(refreshed.id_token ? { id_token: refreshed.id_token } : {}),
      expiry: new Date(
        Date.now() + (refreshed.expires_in ?? 3600) * 1000,
      ).toISOString(),
      token_type: refreshed.token_type ?? "Bearer",
    });
    tokens = await loadStoredTokens(configDirectory);
  }
  const server = serverForArgs(args);
  return configureCliClient(
    server,
    accessToken ?? tokens?.access_token,
    authConfig?.oidc_ca_file ?? process.env.FLEETSHIFT_CA_FILE,
  );
}

export function serverForArgs(args: Parameters<typeof flagString>[0]): string {
  return (
    flagString(
      args,
      "server",
      process.env.FLEETCTL_SERVER ||
        "https://fleetshift-sandbox.localhost:8085",
    ) ?? "https://fleetshift-sandbox.localhost:8085"
  );
}
