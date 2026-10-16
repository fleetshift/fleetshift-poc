import { DynamicRemotePlugin } from "@openshift/dynamic-plugin-sdk-webpack";
import type { Compiler } from "@rspack/core";

import type { ResourceManagerExtension } from "./types";
import { validateExtensionSet } from "./validate";

export type ResourceManagerPluginOptions = {
  pluginMetadata: {
    name: string;
    version: string;
    exposedModules?: Record<string, string>;
  };
  extensions: ResourceManagerExtension[];
  sharedModules?: Record<string, unknown>;
  moduleFederationSettings?: unknown;
  entryCallbackSettings?: unknown;
  entryScriptFilename?: string;
  pluginManifestFilename?: string;
};

export class ResourceManagerPlugin {
  private inner: DynamicRemotePlugin;

  constructor(options: ResourceManagerPluginOptions) {
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
