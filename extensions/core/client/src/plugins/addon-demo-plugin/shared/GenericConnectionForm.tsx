import type { OnboardingActionFormProps } from "@ocm-fleet/common";
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
      <div className="ocm-fleet-setup-whats-next__body">
        <div className="ocm-fleet-setup-whats-next__inner">
          <EmptyState
            headingLevel="h1"
            titleText={`Connect to ${config.providerLabel}`}
          >
            <EmptyStateBody>
              This example shows the {config.providerLabel} connection step.
              Clicking &quot;Complete&quot; simulates a successful connection.
              It does not connect to {config.providerLabel} or provision
              resources.
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
