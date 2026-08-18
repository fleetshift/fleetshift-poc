import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mockNavigate = vi.fn();

const _origWindow = globalThis.window;
const _origDocument = globalThis.document;

beforeEach(() => {
  const store: Record<string, string> = {};
  globalThis.document = { title: "FleetShift" } as unknown as Document;
  globalThis.window = {
    location: {
      origin: "https://fleetshift-sandbox.localhost:8085",
      pathname: "/overview/overview",
    },
    history: {
      replaceState: vi.fn(),
    },
    sessionStorage: {
      getItem: (k: string) => store[k] ?? null,
      setItem: (k: string, v: string) => {
        store[k] = v;
      },
      removeItem: (k: string) => {
        delete store[k];
      },
    },
  } as unknown as typeof globalThis.window;
});

afterEach(() => {
  globalThis.window = _origWindow as unknown as typeof globalThis.window;
  globalThis.document = _origDocument;
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("fetchOidcConfig", () => {
  beforeEach(() => {
    mockNavigate.mockReset();
  });

  it("uses a fixed callback path and restores the deep SPA route", async () => {
    window.sessionStorage.setItem(
      "post_login_redirect_pathname",
      "/app/overview/clusters",
    );
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({
          oidc: {
            authority: "https://fleetshift-sandbox.localhost:8085/idp",
            clientId: "fleetshift-ui",
            scope: "openid profile email groups",
          },
        }),
      }),
    );

    const { fetchOidcConfig } = await import("../oidcConfig");
    const props = await fetchOidcConfig(mockNavigate);

    expect(props.redirect_uri).toBe(
      "https://fleetshift-sandbox.localhost:8085/app/auth/callback",
    );
    expect(props.silent_redirect_uri).toBe(
      "https://fleetshift-sandbox.localhost:8085/app/silent-renew.html",
    );
    expect(props.response_type).toBe("code");
    expect(props.automaticSilentRenew).toBe(true);
    expect(props.authority).toBe(
      "https://fleetshift-sandbox.localhost:8085/idp",
    );
    expect(props.scope).toBe("openid profile email groups");

    props.onSigninCallback?.({} as never);
    expect(window.history.replaceState).toHaveBeenCalled();
    expect(mockNavigate).toHaveBeenCalledWith("/overview/clusters", {
      replace: true,
    });
  });

  it("does not treat the callback path as a post-login destination", async () => {
    window.sessionStorage.setItem(
      "post_login_redirect_pathname",
      "/app/auth/callback",
    );
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({
          oidc: {
            authority: "https://fleetshift-sandbox.localhost:8085/idp",
            clientId: "fleetshift-ui",
            scope: "openid",
          },
        }),
      }),
    );

    const { fetchOidcConfig } = await import("../oidcConfig");
    const props = await fetchOidcConfig(mockNavigate);
    props.onSigninCallback?.({} as never);
    expect(mockNavigate).toHaveBeenCalledWith("/", { replace: true });
  });

  it("omits scope when the server value is empty so oidc-client-ts can default to openid", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({
          oidc: {
            authority: "https://fleetshift-sandbox.localhost:8085/idp",
            clientId: "fleetshift-ui",
            scope: "",
          },
        }),
      }),
    );

    const { fetchOidcConfig } = await import("../oidcConfig");
    const props = await fetchOidcConfig(mockNavigate);
    expect(props).not.toHaveProperty("scope");
  });
});

describe("oidcClientScope", () => {
  it("returns undefined for missing, empty, or whitespace-only values", async () => {
    const { oidcClientScope } = await import("../oidcConfig");
    expect(oidcClientScope(undefined)).toBeUndefined();
    expect(oidcClientScope("")).toBeUndefined();
    expect(oidcClientScope("   ")).toBeUndefined();
  });

  it("passes through a configured scope that already includes openid", async () => {
    const { oidcClientScope } = await import("../oidcConfig");
    expect(oidcClientScope("openid profile email groups")).toBe(
      "openid profile email groups",
    );
  });

  it("prepends openid when a configured scope omits it", async () => {
    const { oidcClientScope } = await import("../oidcConfig");
    expect(oidcClientScope("profile email")).toBe("openid profile email");
  });
});
