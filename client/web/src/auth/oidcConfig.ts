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
    redirect_uri: window.location.origin + "/auth/callback",
    silent_redirect_uri: window.location.origin + "/silent-renew.html",
    post_logout_redirect_uri: window.location.origin + "/",
    // scope: data.oidc.scope || "openid profile email roles",
    // DEx does not suppor roles
    scope: "openid profile email",
    automaticSilentRenew: true,
    onSigninCallback: () => {
      window.history.replaceState({}, document.title, window.location.pathname);
      const postLoginRedirect = sessionStorage.getItem("postLoginRedirect");
      if (postLoginRedirect) {
        // Dex does not allow wildcard redirect URIs, so we need to manually redirect to the original path after login
        window.history.replaceState({}, document.title, postLoginRedirect);
        sessionStorage.removeItem("postLoginRedirect");
      } else {
        // fallback to the root path if no post-login redirect is set
        window.history.replaceState({}, document.title, "/");
      }
    },
  };
}
