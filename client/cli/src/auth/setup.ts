import { flagString, parseArgs } from "../argv";
import { discoverOidc } from "../client";
import { absolutePath, saveAuthConfig } from "../config";

export async function runAuthSetup(
  args: ReturnType<typeof parseArgs>,
): Promise<string> {
  const issuer = flagString(args, "issuer-url").trim();
  const clientID = flagString(args, "client-id").trim();
  if (!issuer || !clientID) {
    throw new Error("--issuer-url and --client-id are required");
  }
  const rawCAFile = flagString(args, "oidc-ca-file");
  const caFile = rawCAFile ? absolutePath(rawCAFile) : undefined;
  const discovered = await discoverOidc(issuer, caFile);
  await saveAuthConfig(flagString(args, "config-dir") || undefined, {
    issuer_url: issuer,
    client_id: clientID,
    scopes: flagString(args, "scopes", "openid,profile,email")
      .split(",")
      .map((scope) => scope.trim())
      .filter(Boolean),
    authorization_endpoint: discovered.authorization_endpoint,
    token_endpoint: discovered.token_endpoint,
    ...(caFile ? { oidc_ca_file: caFile } : {}),
    ...(flagString(args, "key-enrollment-client-id")
      ? {
          key_enrollment_client_id: flagString(
            args,
            "key-enrollment-client-id",
          ),
        }
      : {}),
  });
  return "Local authentication configured. Run 'fleetctl auth login' to authenticate.";
}
