import { ReactNode } from "react";
import {
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
} from "@patternfly/react-core";

export type EnrollmentDLProps = {
  enrollmentName?: ReactNode;
  registry: "github" | "oidc";
  sshPublicKey?: string | null;
};

const EnrollmentDL = ({
  enrollmentName,
  registry,
  sshPublicKey,
}: EnrollmentDLProps) => {
  return (
    <DescriptionList isCompact isHorizontal>
      {enrollmentName && (
        <DescriptionListGroup>
          <DescriptionListTerm>Enrollment</DescriptionListTerm>
          <DescriptionListDescription>
            {enrollmentName}
          </DescriptionListDescription>
        </DescriptionListGroup>
      )}
      <DescriptionListGroup>
        <DescriptionListTerm>Registry</DescriptionListTerm>
        <DescriptionListDescription>
          {registry === "github"
            ? "GitHub SSH signing keys"
            : "OIDC-hosted keys"}
        </DescriptionListDescription>
      </DescriptionListGroup>
      {sshPublicKey && (
        <DescriptionListGroup>
          <DescriptionListTerm>Public key</DescriptionListTerm>
          <DescriptionListDescription>
            <code className="ome-signing-setup__pubkey-snippet">
              {sshPublicKey.slice(0, 60)}...
            </code>
          </DescriptionListDescription>
        </DescriptionListGroup>
      )}
    </DescriptionList>
  );
};

export default EnrollmentDL;
