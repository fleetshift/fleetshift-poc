import type { ExtensionStore } from "./extensionInstall.js";
import type { NavLayoutEntry } from "./navLayout.js";

export interface NavPage {
  id: string;
  scope: string;
  title: string;
}

export interface FleetShiftApi {
  fleetshift: {
    getPluginPagePath: (scope: string, module: string) => string | undefined;
    getNavPages: () => NavPage[];
    /** Backend-defined navigation layout (before user overrides). */
    getBackendLayout: () => NavLayoutEntry[];
    /** Shell-owned extension store — plugins use this instead of getExtensionStore(). */
    extensionStore: ExtensionStore;
  };
}
