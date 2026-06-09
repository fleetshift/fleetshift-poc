import { useMemo, type ComponentType } from "react";
import { useNavigate } from "react-router-dom";
import { useResolvedExtensions } from "@openshift/dynamic-plugin-sdk";
import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import type { ClusterProviderCardProps } from "@fleetshift/common";
import {
  Bullseye,
  Content,
  EmptyState,
  EmptyStateBody,
  Gallery,
  GalleryItem,
  PageSection,
  Spinner,
  Stack,
  StackItem,
  Title,
} from "@patternfly/react-core";

type ClusterProviderExtension = Extension<
  "fleetshift.cluster-provider",
  {
    id: string;
    label: string;
    description: string;
    icon: CodeRef<ComponentType>;
    card: CodeRef<ComponentType<ClusterProviderCardProps>>;
    wizard: CodeRef<ComponentType>;
  }
>;

function isClusterProvider(e: Extension): e is ClusterProviderExtension {
  return e.type === "fleetshift.cluster-provider";
}

export default function ClusterProviderSelectionPage() {
  const [extensions, loaded] = useResolvedExtensions(isClusterProvider);
  const sortedExtensions = useMemo(() => {
    const cpy = [...extensions];
    cpy.sort((a, b) => a.properties.label.localeCompare(b.properties.label));
    return cpy;
  }, [extensions]);
  const navigate = useNavigate();

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
          <Title headingLevel="h1">Create a cluster</Title>
          <Content component="p">
            Select a cluster provider to get started.
          </Content>
        </StackItem>
        <StackItem>
          <Gallery
            className="ome-core-cluster-provider-gallery"
            hasGutter
            minWidths={{ default: "300px" }}
          >
            {sortedExtensions.map((ext) => {
              const CardComponent = ext.properties.card;
              return (
                <GalleryItem key={ext.properties.id}>
                  <CardComponent onSelect={() => navigate(ext.properties.id)} />
                </GalleryItem>
              );
            })}
          </Gallery>
        </StackItem>
      </Stack>
    </PageSection>
  );
}
