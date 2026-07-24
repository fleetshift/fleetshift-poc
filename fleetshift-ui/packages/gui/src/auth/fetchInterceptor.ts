import type { RefObject } from "react";
import type { AuthContextProps } from "react-oidc-context";

let _onUnauthorized: (() => void) | undefined;

const _originalFetch = window.fetch;

export function setOnUnauthorized(handler: () => void) {
  _onUnauthorized = handler;
}

export function installFetchInterceptor(auth: RefObject<AuthContextProps>) {
  window.fetch = function patchedFetch(
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> {
    const url =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.href
          : input.url;

    const isApiRequest =
      url.startsWith("/") || url.startsWith(window.location.origin);

    if (!isApiRequest) {
      return _originalFetch(input, init);
    }

    const token = auth.current?.user?.access_token;
    const headers = new Headers(init?.headers);
    if (token && !headers.has("Authorization")) {
      headers.set("Authorization", `Bearer ${token}`);
    }

    return _originalFetch(input, { ...init, headers }).then((response) => {
      if (response.status === 401 && _onUnauthorized) {
        _onUnauthorized();
      }
      return response;
    });
  };
}
