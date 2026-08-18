import type { AuthProviderProps } from "react-oidc-context";
import { NavigateFunction } from "react-router-dom";

interface UIConfig {
  oidc: {
    authority: string;
    clientId: string;
    scope: string;
    // Optional discovery override. When the server runs its loopback OIDC proxy
    // (built-in Dex sandbox), this points at the proxied .well-known endpoint on
    // our own HTTP origin, so the browser never touches the internal HTTPS
    // issuer and needs no sandbox CA. authority (= issuer) stays unchanged.
    metadataUrl?: string;
  };
}

export async function fetchOidcConfig(
  navigate: NavigateFunction,
): Promise<AuthProviderProps> {
  const res = await fetch("/api/ui/config");
  if (!res.ok) {
    throw new Error(
      `Failed to fetch UI config: ${res.status} ${res.statusText}`,
    );
  }
  const data: UIConfig = await res.json();
  const scope = oidcClientScope(data.oidc.scope);

  // A relative metadataUrl (server-emitted proxy path) resolves against our own
  // origin; an absolute one is passed through untouched.
  const metadataUrl = data.oidc.metadataUrl
    ? new URL(data.oidc.metadataUrl, window.location.origin).toString()
    : undefined;

  return {
    authority: data.oidc.authority,
    metadataUrl,
    client_id: data.oidc.clientId,
    redirect_uri: window.location.origin + "/auth/callback",
    silent_redirect_uri: window.location.origin + "/silent-renew.html",
    post_logout_redirect_uri: window.location.origin + "/",
    scope: data.oidc.scope ?? "",
    automaticSilentRenew: true,
    onSigninCallback: () => {
      let postLoginRedirect = window.sessionStorage.getItem(
        "post_login_redirect_pathname",
      );

      if (!postLoginRedirect || postLoginRedirect === "/auth/callback") {
        postLoginRedirect = "/";
      }

      window.history.replaceState({}, document.title, postLoginRedirect);
      navigate(postLoginRedirect, { replace: true });
    },
  };
}
