import {
  DescriptionList,
  DescriptionListGroup,
  DescriptionListTerm,
  DescriptionListDescription,
  Label,
  Stack,
  StackItem,
  Title,
} from "@patternfly/react-core";
import type { ClusterFormData } from "./CreateClusterWizard";

interface ReviewStepProps {
  formData: ClusterFormData;
}

export default function ReviewStep({ formData }: ReviewStepProps) {
  return (
    <Stack hasGutter>
      <StackItem>
        <Title headingLevel="h3">Review your configuration</Title>
      </StackItem>
      <StackItem>
        <DescriptionList isHorizontal>
          <DescriptionListGroup>
            <DescriptionListTerm>Cluster name</DescriptionListTerm>
            <DescriptionListDescription>
              {formData.name || "—"}
            </DescriptionListDescription>
          </DescriptionListGroup>

          <DescriptionListGroup>
            <DescriptionListTerm>Node image</DescriptionListTerm>
            <DescriptionListDescription>
              {formData.nodeImage || "kind default"}
            </DescriptionListDescription>
          </DescriptionListGroup>

          <DescriptionListGroup>
            <DescriptionListTerm>API server port</DescriptionListTerm>
            <DescriptionListDescription>
              {formData.apiServerPort > 0 ? formData.apiServerPort : "Auto"}
            </DescriptionListDescription>
          </DescriptionListGroup>

          <DescriptionListGroup>
            <DescriptionListTerm>Pod subnet</DescriptionListTerm>
            <DescriptionListDescription>
              {formData.podSubnet || "kind default"}
            </DescriptionListDescription>
          </DescriptionListGroup>

          <DescriptionListGroup>
            <DescriptionListTerm>Service subnet</DescriptionListTerm>
            <DescriptionListDescription>
              {formData.serviceSubnet || "kind default"}
            </DescriptionListDescription>
          </DescriptionListGroup>

          <DescriptionListGroup>
            <DescriptionListTerm>Nodes</DescriptionListTerm>
            <DescriptionListDescription>
              {formData.nodes.map((n, i) => (
                <div key={i}>
                  <Label isCompact>{n.role}</Label>
                  {n.image ? ` — ${n.image}` : ""}
                </div>
              ))}
            </DescriptionListDescription>
          </DescriptionListGroup>

          <DescriptionListGroup>
            <DescriptionListTerm>Signed</DescriptionListTerm>
            <DescriptionListDescription>
              {formData.signDeployment ? "Yes" : "No"}
            </DescriptionListDescription>
          </DescriptionListGroup>

          <DescriptionListGroup>
            <DescriptionListTerm>Placement</DescriptionListTerm>
            <DescriptionListDescription>
              All targets (kind-local)
            </DescriptionListDescription>
          </DescriptionListGroup>
        </DescriptionList>
      </StackItem>
    </Stack>
  );
}
