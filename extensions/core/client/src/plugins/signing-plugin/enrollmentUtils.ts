import type { AuthMethod } from "./api";
import { signDeployment } from "./signingKeyApi";

export type RegistryType = "oidc" | "github";

export async function refreshAndGetIdToken(
  authority: string,
  clientId: string,
): Promise<string> {
  const key = `oidc.user:${authority}:${clientId}`;
  const raw = sessionStorage.getItem(key);
  if (!raw) throw new Error("No OIDC session in sessionStorage");

  const session: Record<string, unknown> = JSON.parse(raw);
  const refreshToken = session.refresh_token as string | undefined;
  if (!refreshToken) throw new Error("No refresh token in session");

  const tokenUrl = `${authority}/protocol/openid-connect/token`;
  const body = new URLSearchParams({
    grant_type: "refresh_token",
    client_id: clientId,
    refresh_token: refreshToken,
  });

  const resp = await fetch(tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Token refresh failed (${resp.status}): ${text}`);
  }

  const tokens = await resp.json();
  const updated = {
    ...session,
    access_token: tokens.access_token,
    id_token: tokens.id_token,
    refresh_token: tokens.refresh_token ?? refreshToken,
  };
  sessionStorage.setItem(key, JSON.stringify(updated));

  if (!tokens.id_token) throw new Error("Refresh did not return an ID token");
  return tokens.id_token as string;
}

export async function testSign(idToken: string): Promise<void> {
  const payload = JSON.stringify({ test: true, ts: Date.now() });
  const payloadBytes = new TextEncoder().encode(payload);
  const signature = await signDeployment(payloadBytes);

  const res = await fetch("/api/ui/verify-sign", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${idToken}`,
    },
    body: JSON.stringify({ payload, signature }),
  });
  if (!res.ok) {
    throw new Error(
      `Signature verification request failed (${res.status}): ${await res.text()}`,
    );
  }
  const result = await res.json();
  if (!result.verified) {
    throw new Error(result.error || "Signature verification failed");
  }
}

export function detectRegistry(method: AuthMethod): RegistryType {
  if (method.oidcConfig?.registrySubjectMapping) return "github";
  return "oidc";
}
