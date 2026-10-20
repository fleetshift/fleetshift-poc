import type { OnboardingActionFormProps } from "@ocm-fleet/common";
import {
  Button,
  EmptyState,
  EmptyStateActions,
  EmptyStateBody,
  EmptyStateFooter,
} from "@patternfly/react-core";

export interface RouteFormConfig {
  routeLabel: string;
}

export function createRouteOnboardingForm(config: RouteFormConfig) {
  return function RouteForm({
    onComplete,
    onCancel,
  }: OnboardingActionFormProps) {
    return (
      <div className="ocm-fleet-setup-whats-next__body">
        <div className="ocm-fleet-setup-whats-next__inner">
          <EmptyState
            headingLevel="h1"
            titleText={`Enable ${config.routeLabel}`}
          >
            <EmptyStateBody>
              Click &quot;Enable&quot; to activate the {config.routeLabel}{" "}
              extension for your fleet.
            </EmptyStateBody>
            <EmptyStateFooter>
              <EmptyStateActions>
                <Button variant="primary" onClick={onComplete}>
                  Enable
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
