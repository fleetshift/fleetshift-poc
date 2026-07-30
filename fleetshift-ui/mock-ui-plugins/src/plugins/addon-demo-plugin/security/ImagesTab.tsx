import { EmptyState, EmptyStateBody } from "@patternfly/react-core";

export default function ImagesTab() {
  return (
    <EmptyState headingLevel="h2" titleText="Images">
      <EmptyStateBody>
        Image vulnerability scanning results will appear here once configured.
      </EmptyStateBody>
    </EmptyState>
  );
}
