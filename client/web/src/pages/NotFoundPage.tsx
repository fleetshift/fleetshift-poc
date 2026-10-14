import {
  Button,
  EmptyState,
  EmptyStateActions,
  EmptyStateBody,
  EmptyStateFooter,
} from "@patternfly/react-core";
import { PathMissingIcon } from "@patternfly/react-icons";
import { Link, useLocation } from "react-router-dom";

export function NotFoundPage() {
  const { pathname } = useLocation();

  return (
    <EmptyState
      headingLevel="h1"
      icon={PathMissingIcon}
      titleText="Page not found"
    >
      <EmptyStateBody>
        The page <strong>{pathname}</strong> does not exist. Check the URL or
        navigate back to the console.
      </EmptyStateBody>
      <EmptyStateFooter>
        <EmptyStateActions>
          <Button
            variant="primary"
            component={(props) => <Link {...props} to="/" />}
          >
            Go to home
          </Button>
        </EmptyStateActions>
      </EmptyStateFooter>
    </EmptyState>
  );
}
