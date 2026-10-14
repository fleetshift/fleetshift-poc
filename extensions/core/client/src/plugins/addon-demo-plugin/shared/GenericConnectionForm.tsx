import type { OnboardingActionFormProps } from "@fleetshift/common";
import {
  Button,
  EmptyState,
  EmptyStateActions,
  EmptyStateBody,
  EmptyStateFooter,
} from "@patternfly/react-core";

export interface ConnectionFormConfig {
  providerLabel: string;
}

export function createConnectionForm(config: ConnectionFormConfig) {
  return function ConnectionForm({
    onComplete,
    onCancel,
  }: OnboardingActionFormProps) {
    return (
      <div className="ome-setup-whats-next__body">
        <div className="ome-setup-whats-next__inner">
          <EmptyState
            headingLevel="h1"
            titleText={`Connect to ${config.providerLabel}`}
          >
            <EmptyStateBody>
              This is a {config.providerLabel} connection demonstration. Click
              &quot;Complete&quot; to finish the demonstration. No connection is
              created.
            </EmptyStateBody>
            <EmptyStateFooter>
              <EmptyStateActions>
                <Button variant="primary" onClick={onComplete}>
                  Complete
                </Button>
                <Button variant="link" onClick={onCancel}>
                  Cancel
                </Button>
              </EmptyStateActions>
            </EmptyStateFooter>
          </EmptyState>
        </div>
      </div>
    );
  };
}
