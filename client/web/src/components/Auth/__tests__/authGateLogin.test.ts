import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const _origWindow = globalThis.window;

beforeEach(() => {
  const store: Record<string, string> = {};
  globalThis.window = {
    location: { pathname: "/app/overview/clusters" },
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
});

describe("shouldAutoStartLogin", () => {
  it("starts login for an unauthenticated visitor", async () => {
    const { shouldAutoStartLogin } = await import("../authGateLogin");
    expect(
      shouldAutoStartLogin({
        loading: false,
        hasUser: false,
        authError: false,
        alreadyTriggered: false,
      }),
    ).toBe(true);
  });

  it("does not auto-start while authError is set", async () => {
    const { shouldAutoStartLogin } = await import("../authGateLogin");
    expect(
      shouldAutoStartLogin({
        loading: false,
        hasUser: false,
        authError: true,
        alreadyTriggered: false,
      }),
    ).toBe(false);
  });

  it("does not auto-start a second time", async () => {
    const { shouldAutoStartLogin } = await import("../authGateLogin");
    expect(
      shouldAutoStartLogin({
        loading: false,
        hasUser: false,
        authError: false,
        alreadyTriggered: true,
      }),
    ).toBe(false);
  });
});

describe("beginLogin", () => {
  it("retries login even after an auth error", async () => {
    const { beginLogin } = await import("../authGateLogin");
    const login = vi.fn();

    beginLogin(login, "/app/overview/clusters");

    expect(login).toHaveBeenCalledTimes(1);
    expect(window.sessionStorage.getItem("post_login_redirect_pathname")).toBe(
      "/app/overview/clusters",
    );
  });
});
