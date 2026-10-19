import { DynamicRemotePlugin } from "@openshift/dynamic-plugin-sdk-webpack";
import type { Compiler } from "@rspack/core";

import type { FleetExtension } from "./types";
import { validateExtensionSet } from "./validate";

export type FleetPluginOptions = {
  pluginMetadata: {
    name: string;
    version: string;
    exposedModules?: Record<string, string>;
  };
  extensions: FleetExtension[];
  sharedModules?: Record<string, unknown>;
  moduleFederationSettings?: unknown;
  entryCallbackSettings?: unknown;
  entryScriptFilename?: string;
  pluginManifestFilename?: string;
};

export class FleetPlugin {
  private inner: DynamicRemotePlugin;

  constructor(options: FleetPluginOptions) {
    const exposedModules = options.pluginMetadata.exposedModules ?? {};

    validateExtensionSet(options.extensions, exposedModules);

    this.inner = new DynamicRemotePlugin(
      options as unknown as ConstructorParameters<
        typeof DynamicRemotePlugin
      >[0],
    );
  }

  apply(compiler: Compiler): void {
    // rspack Compiler is structurally compatible but nominally different from webpack Compiler
    this.inner.apply(compiler as never);
  }
}
