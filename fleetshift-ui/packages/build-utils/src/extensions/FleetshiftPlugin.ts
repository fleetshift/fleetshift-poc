import { DynamicRemotePlugin } from "@openshift/dynamic-plugin-sdk-webpack";
import type { Compiler } from "@rspack/core";

import type { FleetshiftExtension } from "./types";
import { validateExtensionSet } from "./validate";

export type FleetshiftPluginOptions = {
  pluginMetadata: {
    name: string;
    version: string;
    exposedModules?: Record<string, string>;
  };
  extensions: FleetshiftExtension[];
  sharedModules?: Record<string, unknown>;
  moduleFederationSettings?: unknown;
  entryCallbackSettings?: unknown;
  entryScriptFilename?: string;
  pluginManifestFilename?: string;
};

export class FleetshiftPlugin {
  private inner: DynamicRemotePlugin;

  constructor(options: FleetshiftPluginOptions) {
    const exposedModules = options.pluginMetadata.exposedModules ?? {};

    validateExtensionSet(options.extensions, exposedModules);

    this.inner = new DynamicRemotePlugin(
      options as unknown as ConstructorParameters<
        typeof DynamicRemotePlugin
      >[0],
    );
  }

  apply(compiler: Compiler): void {
    this.inner.apply(compiler);
  }
}
