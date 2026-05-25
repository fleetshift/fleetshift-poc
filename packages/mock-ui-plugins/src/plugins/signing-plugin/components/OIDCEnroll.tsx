import {
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
} from "@patternfly/react-core";
import { EnrollStep } from "../enrollmentReducer";

export type OIDCEnrollProps = {
  step: EnrollStep;
  enrollOidc: () => void;
};

const OIDCEnroll = ({ step, enrollOidc }: OIDCEnrollProps) => {
  return (
    <Card isCompact className="pf-v6-u-mt-lg">
      <CardHeader>
        <CardTitle>Register via OIDC provider</CardTitle>
      </CardHeader>
      <CardBody>
        <p>
          Your public key will be stored in your identity provider profile and a
          fresh token issued with the key claim.
        </p>
        <Button
          variant="primary"
          isLoading={step === EnrollStep.Enrolling}
          isDisabled={step === EnrollStep.Enrolling}
          onClick={enrollOidc}
          className="pf-v6-u-mt-sm"
        >
          Enroll signing key
        </Button>
      </CardBody>
    </Card>
  );
};

export default OIDCEnroll;
