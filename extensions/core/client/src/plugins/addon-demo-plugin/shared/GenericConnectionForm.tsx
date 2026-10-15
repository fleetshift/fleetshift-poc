import type { OnboardingActionFormProps } from "@ocm-resource-manager/common";
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
      <div className="ocm-rm-setup-whats-next__body">
        <div className="ocm-rm-setup-whats-next__inner">
          <EmptyState
            headingLevel="h1"
            titleText={`Connect to ${config.providerLabel}`}
          >
            <EmptyStateBody>
              {config.providerLabel} connection configuration is coming soon.
              For now, click &quot;Complete&quot; to simulate a successful
              connection.
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
