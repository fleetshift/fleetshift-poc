import {
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  Label,
  Stack,
  StackItem,
  Title,
} from "@patternfly/react-core";
import type { ReactNode } from "react";

import type { GcpHcpFormData } from "./CreateGcpHcpWizard";

interface ReviewStepProps {
  formData: GcpHcpFormData;
}

interface ReviewField {
  term: string;
  description: ReactNode;
}

function buildReviewFields(formData: GcpHcpFormData): ReviewField[] {
  return [
    { term: "Cluster ID", description: formData.clusterId || "—" },
    { term: "Endpoint access", description: formData.endpointAccess },
    { term: "Release version", description: formData.releaseVersion || "—" },
    {
      term: "Channel group",
      description: <Label isCompact>{formData.channelGroup}</Label>,
    },
    {
      term: "Node pools",
      description: formData.nodepools.map((np, i) => (
        <div key={i}>
          <strong>{np.id || `pool-${i + 1}`}</strong> — {np.replicas} x{" "}
          {np.instanceType}, {np.rootVolumeSize}GB {np.rootVolumeType}
          {np.autoRepair ? ", auto-repair" : ""}
          {`, ${np.upgradeType} upgrade`}
        </div>
      )),
    },
  ];
}

export default function ReviewStep({ formData }: ReviewStepProps) {
  const fields = buildReviewFields(formData);

  return (
    <Stack hasGutter>
      <StackItem>
        <Title headingLevel="h3">Review your configuration</Title>
      </StackItem>
      <StackItem>
        <DescriptionList isHorizontal>
          {fields.map((field) => (
            <DescriptionListGroup key={field.term}>
              <DescriptionListTerm>{field.term}</DescriptionListTerm>
              <DescriptionListDescription>
                {field.description}
              </DescriptionListDescription>
            </DescriptionListGroup>
          ))}
        </DescriptionList>
      </StackItem>
    </Stack>
  );
}
