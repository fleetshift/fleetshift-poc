import { Alert, Button, Title } from "@patternfly/react-core";

export type EnrollmentErrorProps = {
  error: string | null;
  onRetry: () => void;
};

const EnrollmentError = ({ error, onRetry }: EnrollmentErrorProps) => {
  return (
    <div className="fs-setup">
      <Title headingLevel="h1" className="fs-setup__title">
        Signing Key Enrollment
      </Title>
      <Alert variant="danger" isInline title="Enrollment error">
        {error}
      </Alert>
      <Button variant="link" className="pf-v6-u-mt-md" onClick={onRetry}>
        Try again
      </Button>
    </div>
  );
};

export default EnrollmentError;
