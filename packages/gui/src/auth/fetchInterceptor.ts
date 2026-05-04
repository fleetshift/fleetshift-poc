let _token: string | undefined;
let _onUnauthorized: (() => void) | undefined;

const _originalFetch = window.fetch;

export function setAccessToken(token: string | undefined) {
  _token = token;
}

export function setOnUnauthorized(handler: () => void) {
  _onUnauthorized = handler;
}

export function installFetchInterceptor() {
  window.fetch = function patchedFetch(
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> {
    if (!_token) {
      return _originalFetch(input, init);
    }

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

    const headers = new Headers(init?.headers);
    if (!headers.has("Authorization")) {
      headers.set("Authorization", `Bearer ${_token}`);
    }

    return _originalFetch(input, { ...init, headers }).then((response) => {
      if (response.status === 401 && _onUnauthorized) {
        console.warn("[auth] 401 from", url, "— triggering re-auth");
        _onUnauthorized();
      }
      return response;
    });
  };
}
