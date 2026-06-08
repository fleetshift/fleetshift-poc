import type { ComponentType } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useResolvedExtensions } from "@openshift/dynamic-plugin-sdk";
import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import type { ClusterProviderWizardProps } from "@fleetshift/common";
import { usePluginNavigate } from "@fleetshift/common";
import {
  Bullseye,
  EmptyState,
  EmptyStateBody,
  EmptyStateFooter,
  EmptyStateActions,
  Button,
  Spinner,
} from "@patternfly/react-core";

type ClusterProviderExtension = Extension<
  "fleetshift.cluster-provider",
  {
    id: string;
    label: string;
    description: string;
    icon: CodeRef<ComponentType>;
    card: CodeRef<ComponentType>;
    wizard: CodeRef<ComponentType<ClusterProviderWizardProps>>;
  }
>;

function isClusterProvider(e: Extension): e is ClusterProviderExtension {
  return e.type === "fleetshift.cluster-provider";
}

export default function ClusterProviderWizardPage() {
  const { providerId } = useParams<{ providerId: string }>();
  const navigate = useNavigate();
  const clusters = usePluginNavigate("core-plugin", "ClustersModule");
  const [extensions, loaded] = useResolvedExtensions(isClusterProvider);

  if (!loaded) {
    return (
      <Bullseye>
        <Spinner size="xl" />
      </Bullseye>
    );
  }

  const provider = extensions.find((e) => e.properties.id === providerId);

  if (!provider) {
    return (
      <EmptyState headingLevel="h1" titleText="Provider not found">
        <EmptyStateBody>
          The cluster provider &ldquo;{providerId}&rdquo; is not available.
        </EmptyStateBody>
        <EmptyStateFooter>
          <EmptyStateActions>
            <Button variant="primary" onClick={() => navigate(-1)}>
              Back to provider selection
            </Button>
          </EmptyStateActions>
        </EmptyStateFooter>
      </EmptyState>
    );
  }

  const WizardComponent = provider.properties.wizard;

  return <WizardComponent onSetupNext={() => clusters.navigate()} />;
}
