import { getUiConfig } from "@fleetshift/common";
import type { AuthProviderNoUserManagerProps } from "react-oidc-context";
import type { NavigateFunction } from "react-router-dom";

import apiClient, { unwrap } from "../api/client";
import {
  APP_BASENAME,
  AUTH_CALLBACK_PATH,
  isAuthCallbackPath,
  SILENT_RENEW_PATH,
  stripAppBasename,
  toBrowserPath,
} from "../appBase";

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

export async function fetchOidcConfig(
  navigate: NavigateFunction,
): Promise<AuthProviderNoUserManagerProps> {
  const data = await unwrap(getUiConfig({ client: apiClient }));
  const scope = oidcClientScope(data.oidc.scope);

  return {
    authority: data.oidc.authority,
    client_id: data.oidc.clientId,
    redirect_uri: window.location.origin + AUTH_CALLBACK_PATH,
    silent_redirect_uri: window.location.origin + SILENT_RENEW_PATH,
    post_logout_redirect_uri: window.location.origin + APP_BASENAME + "/",
    response_type: "code",
    ...(scope !== undefined ? { scope } : {}),
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
