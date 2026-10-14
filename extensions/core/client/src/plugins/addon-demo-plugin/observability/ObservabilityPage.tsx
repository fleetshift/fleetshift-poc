import {
  EmptyState,
  EmptyStateBody,
  PageSection,
  Title,
} from "@patternfly/react-core";

const ObservabilityPage = () => {
  return (
    <PageSection>
      <Title headingLevel="h1" className="pf-v6-u-mb-lg">
        Observability
      </Title>
      <EmptyState headingLevel="h2" titleText="Coming soon">
        <EmptyStateBody>
          Unified metrics, logs, and traces across your fleet. Monitor cluster
          health, application performance, and resource utilization from a
          single pane of glass.
        </EmptyStateBody>
      </EmptyState>
    </PageSection>
  );
};

export default ObservabilityPage;
