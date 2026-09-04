import { uiConfigStore } from "../utils/uiConfig";

export type OIDCConfig = {
  name?: string;
  authority: string;
  clientId: string;
  scope?: string;
  emailDomain?: string;
};

export interface UIConfig {
  oidc: OIDCConfig[];
}

// oidc-client-ts supplies `openid` only when `scope` is omitted, not when it is "".
export function oidcClientScope(raw: string | undefined): string | undefined {
  const tokens = (raw ?? "")
    .trim()
    .split(/\s+/)
    .filter((t) => t.length > 0);
  if (tokens.length === 0) {
    return undefined;
  }
  if (!tokens.includes("openid")) {
    tokens.unshift("openid");
  }
  return tokens.join(" ");
}

export async function fetchOidcConfig(): Promise<void> {
  const res = await fetch("/api/ui/config");
  if (!res.ok) {
    throw new Error(
      `Failed to fetch UI config: ${res.status} ${res.statusText}`,
    );
  }
  const data: UIConfig = await res.json();
  // Update the UI config store with the fetched data before proceeding.
  uiConfigStore.updateState("INIT", data);
}
