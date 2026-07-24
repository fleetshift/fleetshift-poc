import { EmptyState, EmptyStateBody } from "@patternfly/react-core";

export default function DeploymentsTab() {
  return (
    <EmptyState headingLevel="h2" titleText="Deployments">
      <EmptyStateBody>
        Deployment security posture details will appear here once configured.
      </EmptyStateBody>
    </EmptyState>
  );
}
