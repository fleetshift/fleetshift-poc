import type { AuthProviderProps } from "react-oidc-context";

interface UIConfig {
  oidc: {
    authority: string;
    clientId: string;
    scope: string;
  };
}

export async function fetchOidcConfig(): Promise<AuthProviderProps> {
  const res = await fetch("/api/ui/config");
  if (!res.ok) {
    throw new Error(
      `Failed to fetch UI config: ${res.status} ${res.statusText}`,
    );
  }
  const data: UIConfig = await res.json();

  return {
    authority: data.oidc.authority,
    client_id: data.oidc.clientId,
    redirect_uri: window.location.origin + window.location.pathname,
    silent_redirect_uri: window.location.origin + "/silent-renew.html",
    post_logout_redirect_uri: window.location.origin + "/",
    scope: data.oidc.scope || "openid profile email roles",
    automaticSilentRenew: true,
    onSigninCallback: () => {
      window.history.replaceState({}, document.title, window.location.pathname);
    },
  };
}
