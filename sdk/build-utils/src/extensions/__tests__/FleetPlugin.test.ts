import { describe, expect, it, vi } from "vitest";

import { createRoute, createRouteGroup, createSetup } from "../builders";
import { FleetPlugin } from "../FleetPlugin";

const { applySpy } = vi.hoisted(() => ({ applySpy: vi.fn() }));

vi.mock("@openshift/dynamic-plugin-sdk-webpack", () => ({
  DynamicRemotePlugin: class {
    options: unknown;
    constructor(options: unknown) {
      this.options = options;
    }
    apply(compiler: unknown) {
      applySpy(compiler);
    }
  },
}));

const baseOptions = {
  pluginMetadata: {
    name: "test-plugin",
    version: "1.0.0",
    exposedModules: {
      TestPage: "./src/TestPage.tsx",
      TestIcon: "./src/TestIcon.tsx",
    },
  },
  sharedModules: {},
  entryScriptFilename: "plugins/test/test-plugin.js",
  pluginManifestFilename: "plugins/test/test-plugin-manifest.json",
};

describe("FleetPlugin", () => {
  it("constructs with valid extensions", () => {
    expect(
      () =>
        new FleetPlugin({
          ...baseOptions,
          extensions: [
            createRoute({
              id: "test",
              label: "Test",
              component: { $codeRef: "TestPage.default" },
              icon: { $codeRef: "TestIcon.default" },
            }),
          ],
        }),
    ).not.toThrow();
  });

  it("throws when CodeRef references missing exposed module", () => {
    expect(
      () =>
        new FleetPlugin({
          ...baseOptions,
          extensions: [
            createRoute({
              id: "test",
              label: "Test",
              component: { $codeRef: "MissingModule.default" },
              icon: { $codeRef: "TestIcon.default" },
            }),
          ],
        }),
    ).toThrow(/MissingModule.*not in exposedModules/);
  });

  it("throws on duplicate extension IDs", () => {
    expect(
      () =>
        new FleetPlugin({
          ...baseOptions,
          pluginMetadata: {
            ...baseOptions.pluginMetadata,
            exposedModules: {
              PageA: "./src/A.tsx",
              PageB: "./src/B.tsx",
              TestIcon: "./src/TestIcon.tsx",
            },
          },
          extensions: [
            createRoute({
              id: "same-id",
              label: "First",
              component: { $codeRef: "PageA.default" },
              icon: { $codeRef: "TestIcon.default" },
            }),
            createRoute({
              id: "same-id",
              label: "Second",
              component: { $codeRef: "PageB.default" },
              icon: { $codeRef: "TestIcon.default" },
            }),
          ],
        }),
    ).toThrow(/Duplicate extension ID "same-id"/);
  });

  it("accepts mixed extension types", () => {
    expect(
      () =>
        new FleetPlugin({
          ...baseOptions,
          pluginMetadata: {
            ...baseOptions.pluginMetadata,
            exposedModules: {
              TestPage: "./src/TestPage.tsx",
              TestIcon: "./src/TestIcon.tsx",
              SetupForm: "./src/SetupForm.tsx",
            },
          },
          extensions: [
            createRoute({
              id: "test-route",
              label: "Test",
              component: { $codeRef: "TestPage.default" },
              icon: { $codeRef: "TestIcon.default" },
            }),
            createSetup({
              id: "test-setup",
              label: "Setup",
              path: "setup",
              component: { $codeRef: "SetupForm.default" },
              requires: [],
              requiresAuth: false,
            }),
          ],
        }),
    ).not.toThrow();
  });

  it("constructs with route group and grouped routes", () => {
    expect(
      () =>
        new FleetPlugin({
          ...baseOptions,
          pluginMetadata: {
            ...baseOptions.pluginMetadata,
            exposedModules: {
              TestPage: "./src/TestPage.tsx",
              TestIcon: "./src/TestIcon.tsx",
              GroupIcon: "./src/GroupIcon.tsx",
            },
          },
          extensions: [
            createRouteGroup({
              id: "settings",
              label: "Settings",
            }),
            createRoute({
              id: "navigation",
              label: "Navigation",
              component: { $codeRef: "TestPage.default" },
              icon: { $codeRef: "TestIcon.default" },
              group: "settings",
            }),
          ],
        }),
    ).not.toThrow();
  });

  it("throws when route references non-existent group", () => {
    expect(
      () =>
        new FleetPlugin({
          ...baseOptions,
          extensions: [
            createRoute({
              id: "navigation",
              label: "Navigation",
              component: { $codeRef: "TestPage.default" },
              icon: { $codeRef: "TestIcon.default" },
              group: "nonexistent",
            }),
          ],
        }),
    ).toThrow(/references group "nonexistent"/);
  });

  it("accepts empty extensions array", () => {
    expect(
      () =>
        new FleetPlugin({
          ...baseOptions,
          extensions: [],
        }),
    ).not.toThrow();
  });

  it("delegates apply to inner plugin", () => {
    applySpy.mockClear();
    const plugin = new FleetPlugin({
      ...baseOptions,
      extensions: [],
    });
    const mockCompiler = {} as unknown as import("@rspack/core").Compiler;
    plugin.apply(mockCompiler);
    expect(applySpy).toHaveBeenCalledTimes(1);
    expect(applySpy).toHaveBeenCalledWith(mockCompiler);
  });
});
