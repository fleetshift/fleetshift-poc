import { flagString } from "../argv";
import { oidcRequest, type TokenResponse } from "../auth/helpers";
import { type CliClient, createCliClient } from "../client";
import { loadAuthConfig, loadStoredTokens, saveStoredTokens } from "../config";

export async function clientForArgs(
  args: Parameters<typeof flagString>[0],
): Promise<CliClient> {
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
  return createCliClient(
    server,
    tokens?.access_token,
    authConfig?.oidc_ca_file ?? process.env.FLEETSHIFT_CA_FILE,
  );
}

export function serverForArgs(args: Parameters<typeof flagString>[0]): string {
  return flagString(
    args,
    "server",
    process.env.FLEETCTL_SERVER || "https://fleetshift-sandbox.localhost:8085",
  );
}
