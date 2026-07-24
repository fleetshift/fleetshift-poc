import type { RefObject } from "react";
import type { AuthContextProps } from "react-oidc-context";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mockOriginalFetch = vi.fn().mockResolvedValue(new Response("ok"));

const _origWindow = globalThis.window;

beforeEach(() => {
  globalThis.window = {
    fetch: mockOriginalFetch,
    location: { origin: "http://localhost:3000" },
  } as unknown as typeof globalThis.window;
});

afterEach(() => {
  globalThis.window = _origWindow as unknown as typeof globalThis.window;
});

const loadModule = () => import("../fetchInterceptor");

function fakeAuthRef(token?: string): RefObject<AuthContextProps> {
  return {
    current: {
      user: token
        ? ({ access_token: token } as AuthContextProps["user"])
        : null,
    } as AuthContextProps,
  };
}

describe("GUI fetchInterceptor", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockOriginalFetch.mockResolvedValue(new Response("ok"));
  });

  it("adds Authorization header to same-origin requests", async () => {
    const { installFetchInterceptor } = await loadModule();
    installFetchInterceptor(fakeAuthRef("gui-token-abc"));

    await window.fetch("http://localhost:3000/api/v1/clusters");

    expect(mockOriginalFetch).toHaveBeenCalled();
    const [, init] = mockOriginalFetch.mock.calls[0];
    const headers = new Headers(init?.headers);
    expect(headers.get("Authorization")).toBe("Bearer gui-token-abc");
  });

  it("adds Authorization header to relative path requests", async () => {
    const { installFetchInterceptor } = await loadModule();
    installFetchInterceptor(fakeAuthRef("gui-token-abc"));

    await window.fetch("/v1/clusters");

    expect(mockOriginalFetch).toHaveBeenCalled();
    const [, init] = mockOriginalFetch.mock.calls[0];
    const headers = new Headers(init?.headers);
    expect(headers.get("Authorization")).toBe("Bearer gui-token-abc");
  });

  it("does not add header for non-matching URLs", async () => {
    const { installFetchInterceptor } = await loadModule();
    installFetchInterceptor(fakeAuthRef("gui-token-abc"));

    await window.fetch("https://example.com/other");

    expect(mockOriginalFetch).toHaveBeenCalled();
    const [, init] = mockOriginalFetch.mock.calls[0];
    const headers = new Headers(init?.headers);
    expect(headers.get("Authorization")).toBeNull();
  });

  it("does not add header when token is undefined", async () => {
    const { installFetchInterceptor } = await loadModule();
    installFetchInterceptor(fakeAuthRef(undefined));

    await window.fetch("http://localhost:3000/api/v1/clusters");

    expect(mockOriginalFetch).toHaveBeenCalled();
    const [, init] = mockOriginalFetch.mock.calls[0];
    const headers = new Headers(init?.headers);
    expect(headers.get("Authorization")).toBeNull();
  });

  it("picks up token changes via the ref", async () => {
    const { installFetchInterceptor } = await loadModule();
    const ref = fakeAuthRef(undefined);
    installFetchInterceptor(ref);

    await window.fetch("/v1/clusters");
    const [, init1] = mockOriginalFetch.mock.calls[0];
    expect(new Headers(init1?.headers).get("Authorization")).toBeNull();

    (ref.current as AuthContextProps).user = {
      access_token: "fresh-token",
    } as AuthContextProps["user"];

    await window.fetch("/v1/clusters");
    const [, init2] = mockOriginalFetch.mock.calls[1];
    expect(new Headers(init2?.headers).get("Authorization")).toBe(
      "Bearer fresh-token",
    );
  });
});
