import type { AuthProviderNoUserManagerProps } from "react-oidc-context";
import type { NavigateFunction } from "react-router-dom";

import {
  APP_BASENAME,
  AUTH_CALLBACK_PATH,
  SILENT_RENEW_PATH,
  isAuthCallbackPath,
  stripAppBasename,
  toBrowserPath,
} from "../appBase";

interface UIConfig {
  oidc: {
    authority: string;
    clientId: string;
    scope: string;
  };
}

export async function fetchOidcConfig(
  navigate: NavigateFunction,
): Promise<AuthProviderNoUserManagerProps> {
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
    redirect_uri: window.location.origin + AUTH_CALLBACK_PATH,
    silent_redirect_uri: window.location.origin + SILENT_RENEW_PATH,
    post_logout_redirect_uri: window.location.origin + APP_BASENAME + "/",
    response_type: "code",
    scope: data.oidc.scope ?? "",
    automaticSilentRenew: true,
    onSigninCallback: () => {
      let postLoginRedirect = window.sessionStorage.getItem(
        "post_login_redirect_pathname",
      );

      if (!postLoginRedirect || isAuthCallbackPath(postLoginRedirect)) {
        postLoginRedirect = `${APP_BASENAME}/`;
      }

      const browserPath = toBrowserPath(postLoginRedirect);
      window.history.replaceState({}, document.title, browserPath);
      navigate(stripAppBasename(browserPath), { replace: true });
    },
  };
}
