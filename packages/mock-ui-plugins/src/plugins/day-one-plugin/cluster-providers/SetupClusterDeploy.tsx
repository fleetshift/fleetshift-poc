import { useMemo } from "react";
import type { ComponentType } from "react";
import { Routes, Route, useNavigate, useParams } from "react-router-dom";
import { useResolvedExtensions } from "@openshift/dynamic-plugin-sdk";
import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import type {
  ClusterProviderCardProps,
  ClusterProviderWizardProps,
} from "@fleetshift/common";
import {
  Bullseye,
  Button,
  Content,
  EmptyState,
  EmptyStateBody,
  EmptyStateFooter,
  EmptyStateActions,
  Gallery,
  GalleryItem,
  PageSection,
  Spinner,
  Stack,
  StackItem,
  Title,
} from "@patternfly/react-core";

import "./SetupClusterDeploy.scss";

type ClusterProviderExtension = Extension<
  "fleetshift.cluster-provider",
  {
    id: string;
    label: string;
    description: string;
    icon: CodeRef<ComponentType>;
    card: CodeRef<ComponentType<ClusterProviderCardProps>>;
    wizard: CodeRef<ComponentType<ClusterProviderWizardProps>>;
  }
>;

function isClusterProvider(e: Extension): e is ClusterProviderExtension {
  return e.type === "fleetshift.cluster-provider";
}

interface SetupClusterDeployProps {
  onSetupNext?: () => void;
  onSetupSkip?: () => void;
}

interface ProviderGalleryProps {
  onSetupSkip?: () => void;
}

function ProviderGallery({ onSetupSkip }: ProviderGalleryProps) {
  const [extensions, loaded] = useResolvedExtensions(isClusterProvider);
  const navigate = useNavigate();
  const sortedExtensions = useMemo(() => {
    const cpy = [...extensions];
    cpy.sort((a, b) => a.properties.label.localeCompare(b.properties.label));
    return cpy;
  }, [extensions]);

  if (!loaded) {
    return (
      <Bullseye>
        <Spinner size="xl" />
      </Bullseye>
    );
  }

  if (extensions.length === 0) {
    return (
      <EmptyState headingLevel="h1" titleText="No cluster providers available">
        <EmptyStateBody>
          No cluster provider plugins are currently registered.
        </EmptyStateBody>
      </EmptyState>
    );
  }

  return (
    <PageSection>
      <Stack hasGutter>
        <StackItem>
          <Title headingLevel="h1">Deploy a cluster</Title>
          <Content component="p">
            Select a cluster provider to get started.
          </Content>
        </StackItem>
        <StackItem>
          <Gallery hasGutter minWidths={{ default: "300px" }}>
            {sortedExtensions.map((ext) => {
              const CardComponent = ext.properties.card;
              return (
                <GalleryItem
                  className="ome-day-one-cluster-provider-gallery-item"
                  key={ext.properties.id}
                >
                  <CardComponent
                    onSelect={() =>
                      navigate(ext.properties.id, { relative: "path" })
                    }
                  />
                </GalleryItem>
              );
            })}
          </Gallery>
        </StackItem>
        {onSetupSkip && (
          <StackItem>
            <Button variant="link" onClick={onSetupSkip}>
              Skip to console
            </Button>
          </StackItem>
        )}
      </Stack>
    </PageSection>
  );
}

interface ProviderWizardProps {
  onSetupNext?: () => void;
  onSetupSkip?: () => void;
}

function ProviderWizard({ onSetupNext, onSetupSkip }: ProviderWizardProps) {
  const { providerId } = useParams<{ providerId: string }>();
  const navigate = useNavigate();
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
            <Button variant="primary" onClick={() => navigate("..")}>
              Back to provider selection
            </Button>
          </EmptyStateActions>
        </EmptyStateFooter>
      </EmptyState>
    );
  }

  const WizardComponent = provider.properties.wizard;
  return (
    <WizardComponent onSetupNext={onSetupNext} onSetupSkip={onSetupSkip} />
  );
}

export default function SetupClusterDeploy({
  onSetupNext,
  onSetupSkip,
}: SetupClusterDeployProps) {
  return (
    <Routes>
      <Route index element={<ProviderGallery onSetupSkip={onSetupSkip} />} />
      <Route
        path=":providerId"
        element={
          <ProviderWizard onSetupNext={onSetupNext} onSetupSkip={onSetupSkip} />
        }
      />
    </Routes>
  );
}
