import type { AuthProviderProps } from "react-oidc-context";
import { NavigateFunction } from "react-router-dom";

interface UIConfig {
  oidc: {
    authority: string;
    clientId: string;
    scope?: string;
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

  return {
    authority: data.oidc.authority,
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
