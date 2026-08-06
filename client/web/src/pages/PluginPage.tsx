import { useExtensionInstall } from "@fleetshift/common";
import {
  Bullseye,
  EmptyState,
  EmptyStateBody,
  Spinner,
} from "@patternfly/react-core";
import { CubesIcon } from "@patternfly/react-icons";
import { ScalprumComponent, useScalprum } from "@scalprum/react-core";
import { useCallback } from "react";

import { useAppConfig } from "../contexts/AppConfigContext";
import { useScope } from "../contexts/ScopeContext";
import { pluginKeyFromName } from "../utils/extensions";
import { ExtensionEnablePage } from "./ExtensionEnablePage";

interface PluginPageProps {
  scope: string;
  module: string;
  pluginKey?: string;
}

export const PluginPage = ({ scope, module, pluginKey }: PluginPageProps) => {
  const { config: scalprumConfig } = useScalprum();
  const { clusterIdsForPlugin } = useScope();
  const {
    isInstalled,
    install,
    loaded: installLoaded,
    error: installError,
  } = useExtensionInstall();
  const { pluginPages } = useAppConfig();

  const key = pluginKey ?? pluginKeyFromName(scope);
  const clusterIds = clusterIdsForPlugin(key);
  const isAvailable = scope in scalprumConfig;

  const page = pluginPages.find((p) => p.scope === scope);

  const handleInstall = useCallback(() => {
    install(scope);
  }, [install, scope]);

  if (!isAvailable) {
    return (
      <EmptyState
        icon={CubesIcon}
        headingLevel="h2"
        titleText="Plugin unavailable"
      >
        <EmptyStateBody>
          The <strong>{scope}</strong> plugin is not currently loaded. It may
          not be enabled on any connected cluster.
        </EmptyStateBody>
      </EmptyState>
    );
  }

  if (!installLoaded) {
    return (
      <Bullseye>
        <Spinner size="xl" />
      </Bullseye>
    );
  }

  if (installError) {
    return (
      <EmptyState
        icon={CubesIcon}
        headingLevel="h2"
        titleText="Extension state unavailable"
      >
        <EmptyStateBody>
          Failed to load extension state. The plugin may still work — try
          refreshing the page.
        </EmptyStateBody>
      </EmptyState>
    );
  }

  if (!isInstalled(scope)) {
    return (
      <ExtensionEnablePage
        label={page?.title ?? scope}
        description={`Enable ${page?.title ?? scope} to access its features.`}
        onInstall={handleInstall}
      />
    );
  }

  return (
    <ScalprumComponent
      key={`${scope}:${module}:${clusterIds.join(",")}`}
      scope={scope}
      module={module}
      fallback={
        <Bullseye>
          <Spinner size="xl" />
        </Bullseye>
      }
      ErrorComponent={
        <EmptyState
          icon={CubesIcon}
          headingLevel="h2"
          titleText="Failed to load"
        >
          <EmptyStateBody>
            An error occurred loading <strong>{module}</strong> from{" "}
            <strong>{scope}</strong>.
          </EmptyStateBody>
        </EmptyState>
      }
      {...{ clusterIds }}
    />
  );
};
