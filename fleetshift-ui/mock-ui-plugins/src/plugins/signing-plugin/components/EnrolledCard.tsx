import {
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Icon,
  Title,
} from "@patternfly/react-core";
import { CheckCircleIcon } from "@patternfly/react-icons";

import EnrollmentDL, { EnrollmentDLProps } from "./EnrollmentDL";

export type EnrolledCardProps = EnrollmentDLProps & {
  isSetupFlow?: boolean;
  handleReenroll: () => void;
};

const EnrolledCard = ({
  registry,
  enrollmentName,
  sshPublicKey,
  isSetupFlow = false,
  handleReenroll,
}: EnrolledCardProps) => {
  return (
    <div className="ome-signing-setup">
      <Title headingLevel="h1" className="ome-signing-setup__title">
        Signing Key Enrollment
      </Title>
      <Card isCompact className="ome-signing-setup__configured-card">
        <CardHeader>
          <CardTitle>
            <Icon status="success">
              <CheckCircleIcon />
            </Icon>{" "}
            Signing key enrolled and verified
          </CardTitle>
        </CardHeader>
        <CardBody>
          <EnrollmentDL
            registry={registry}
            enrollmentName={enrollmentName}
            sshPublicKey={sshPublicKey}
          />
        </CardBody>
      </Card>
      <div className="pf-v6-u-mt-xl">
        {isSetupFlow && (
          <Button
            variant="primary"
            component="a"
            href="/"
            className="pf-v6-u-mr-md"
          >
            Continue to console
          </Button>
        )}
        <Button variant="secondary" onClick={handleReenroll}>
          Re-enroll
        </Button>
      </div>
    </div>
  );
};

export default EnrolledCard;
