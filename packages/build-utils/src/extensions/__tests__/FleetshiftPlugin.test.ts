import { describe, expect, it, vi } from "vitest";

import { createModule, createModuleGroup, createSetup } from "../builders";
import { FleetshiftPlugin } from "../FleetshiftPlugin";

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

describe("FleetshiftPlugin", () => {
  it("constructs with valid extensions", () => {
    expect(
      () =>
        new FleetshiftPlugin({
          ...baseOptions,
          extensions: [
            createModule({
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
        new FleetshiftPlugin({
          ...baseOptions,
          extensions: [
            createModule({
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
        new FleetshiftPlugin({
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
            createModule({
              id: "same-id",
              label: "First",
              component: { $codeRef: "PageA.default" },
              icon: { $codeRef: "TestIcon.default" },
            }),
            createModule({
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
        new FleetshiftPlugin({
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
            createModule({
              id: "test-module",
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

  it("constructs with module group and grouped modules", () => {
    expect(
      () =>
        new FleetshiftPlugin({
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
            createModuleGroup({
              id: "settings",
              label: "Settings",
            }),
            createModule({
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

  it("throws when module references non-existent group", () => {
    expect(
      () =>
        new FleetshiftPlugin({
          ...baseOptions,
          extensions: [
            createModule({
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
        new FleetshiftPlugin({
          ...baseOptions,
          extensions: [],
        }),
    ).not.toThrow();
  });

  it("delegates apply to inner plugin", () => {
    applySpy.mockClear();
    const plugin = new FleetshiftPlugin({
      ...baseOptions,
      extensions: [],
    });
    const mockCompiler = {} as unknown as import("@rspack/core").Compiler;
    plugin.apply(mockCompiler);
    expect(applySpy).toHaveBeenCalledTimes(1);
    expect(applySpy).toHaveBeenCalledWith(mockCompiler);
  });
});
