import { flagString, parseArgs } from "../argv";
import { loadStoredTokens } from "../config";

interface DecodedJWT {
  header: Record<string, unknown>;
  claims: Record<string, unknown>;
}

interface TokenInspection {
  token_type: string;
  expiry: string;
  status: string;
  has_refresh_token: boolean;
  access_token?: DecodedJWT;
  id_token?: DecodedJWT;
}

export async function inspectStoredTokens(
  args: ReturnType<typeof parseArgs>,
): Promise<TokenInspection> {
  let tokens: Awaited<ReturnType<typeof loadStoredTokens>>;
  try {
    tokens = await loadStoredTokens(
      flagString(args, "config-dir") || undefined,
    );
  } catch {
    throw new Error("unable to load tokens; run auth login first");
  }
  const expiry = new Date(tokens.expiry);
  const remaining = expiry.getTime() - Date.now();
  const result: TokenInspection = {
    token_type: tokens.token_type,
    expiry: tokens.expiry,
    status:
      remaining > 0
        ? `Valid (expires in ${formatDuration(remaining)})`
        : `Expired (${formatDuration(-remaining)} ago)`,
    has_refresh_token: Boolean(tokens.refresh_token),
  };
  if (tokens.access_token) {
    try {
      result.access_token = decodeJWT(tokens.access_token);
    } catch {
      // Opaque access tokens are valid; claims are optional inspection data.
    }
  }
  if (tokens.id_token) {
    try {
      result.id_token = decodeJWT(tokens.id_token);
    } catch {
      // Keep token metadata available when provider returns a non-JWT token.
    }
  }
  return result;
}

export function formatTokenInspection(
  inspection: TokenInspection,
  output: string,
): string {
  if (output === "json") return JSON.stringify(inspection, null, 2);
  const lines = [
    `Token Type:     ${inspection.token_type}`,
    `Expiry:         ${inspection.expiry}`,
    `Status:         ${inspection.status}`,
    `Refresh Token:  ${inspection.has_refresh_token}`,
  ];
  if (inspection.access_token) {
    lines.push(
      "",
      "Access Token Claims:",
      ...formatClaims(inspection.access_token.claims),
    );
  }
  if (inspection.id_token) {
    lines.push(
      "",
      "ID Token Claims:",
      ...formatClaims(inspection.id_token.claims),
    );
  }
  return lines.join("\n");
}

function decodeJWT(token: string): DecodedJWT {
  const parts = token.split(".");
  if (parts.length !== 3) throw new Error("invalid JWT: expected 3 segments");
  return {
    header: JSON.parse(
      Buffer.from(parts[0], "base64url").toString("utf8"),
    ) as Record<string, unknown>,
    claims: JSON.parse(
      Buffer.from(parts[1], "base64url").toString("utf8"),
    ) as Record<string, unknown>,
  };
}

function formatClaims(claims: Record<string, unknown>): string[] {
  const known = [
    "sub",
    "iss",
    "aud",
    "exp",
    "iat",
    "nbf",
    "email",
    "groups",
    "azp",
  ];
  const keys = [...new Set([...known, ...Object.keys(claims)])].filter(
    (key) => key in claims,
  );
  return keys.map(
    (key) => `  ${`${key}:`.padEnd(13)}${formatClaim(key, claims[key])}`,
  );
}

function formatClaim(key: string, value: unknown): string {
  if (["exp", "iat", "nbf"].includes(key) && typeof value === "number") {
    return new Date(value * 1000).toISOString().replace(".000Z", "Z");
  }
  if (Array.isArray(value)) return value.map(String).join(", ");
  if (typeof value === "object" && value !== null) return JSON.stringify(value);
  return String(value);
}

function formatDuration(milliseconds: number): string {
  const seconds = Math.round(milliseconds / 1000);
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor(seconds / 60) % 60;
  const rest = seconds % 60;
  if (hours > 0) return `${hours}h${minutes}m`;
  if (minutes > 0) return `${minutes}m${rest}s`;
  return `${rest}s`;
}
