import {
  Button,
  EmptyState,
  EmptyStateActions,
  EmptyStateBody,
  EmptyStateFooter,
  PageSection,
  Title,
} from "@patternfly/react-core";

interface AuthErrorStateProps {
  onSignIn: () => void;
}

export const AuthErrorState = ({ onSignIn }: AuthErrorStateProps) => (
  <PageSection isFilled>
    <EmptyState variant="lg">
      <Title headingLevel="h1" size="lg">
        Unauthorized
      </Title>
      <EmptyStateBody>
        Your authentication session is no longer valid. Please sign in again to
        continue.
      </EmptyStateBody>
      <EmptyStateFooter>
        <EmptyStateActions>
          <Button variant="primary" onClick={onSignIn}>
            Sign in
          </Button>
        </EmptyStateActions>
      </EmptyStateFooter>
    </EmptyState>
  </PageSection>
);
