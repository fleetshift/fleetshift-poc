import {
  Form,
  FormGroup,
  FormSelect,
  FormSelectOption,
  TextInput,
} from "@patternfly/react-core";
import type { GcpHcpFormData } from "./CreateGcpHcpWizard";

const CLUSTER_ID_PATTERN = /^[a-z][-a-z0-9]*$/;

interface ClusterDetailsStepProps {
  formData: GcpHcpFormData;
  onChange: <K extends keyof GcpHcpFormData>(
    field: K,
    value: GcpHcpFormData[K],
  ) => void;
}

export default function ClusterDetailsStep({
  formData,
  onChange,
}: ClusterDetailsStepProps) {
  return (
    <Form>
      <FormGroup
        label="Cluster ID"
        isRequired
        fieldId="cluster-id"
        // helperText="Must start with a lowercase letter and contain only lowercase letters, digits, and hyphens."
        // helperTextInvalid="Must start with a lowercase letter and contain only lowercase letters, digits, and hyphens."
        // validated={
        //   !formData.clusterId.trim() || CLUSTER_ID_PATTERN.test(formData.clusterId.trim())
        //     ? "default"
        //     : "error"
        // }
      >
        <TextInput
          id="cluster-id"
          isRequired
          value={formData.clusterId}
          onChange={(_e, val) => onChange("clusterId", val)}
          placeholder="my-hcp-cluster"
          validated={
            !formData.clusterId.trim()
              ? "default"
              : CLUSTER_ID_PATTERN.test(formData.clusterId.trim())
                ? "default"
                : "error"
          }
        />
      </FormGroup>

      <FormGroup label="Endpoint access" isRequired fieldId="endpoint-access">
        <FormSelect
          id="endpoint-access"
          value={formData.endpointAccess}
          onChange={(_e, val) => onChange("endpointAccess", val)}
        >
          <FormSelectOption
            value="PublicAndPrivate"
            label="Public and Private"
          />
          <FormSelectOption value="Public" label="Public" />
          <FormSelectOption value="Private" label="Private" />
        </FormSelect>
      </FormGroup>

      <FormGroup label="Release version" isRequired fieldId="release-version">
        <TextInput
          id="release-version"
          isRequired
          value={formData.releaseVersion}
          onChange={(_e, val) => onChange("releaseVersion", val)}
          placeholder="4.22.0"
          validated={formData.releaseVersion.trim() ? "default" : "error"}
        />
      </FormGroup>

      <FormGroup label="Channel group" isRequired fieldId="channel-group">
        <FormSelect
          id="channel-group"
          value={formData.channelGroup}
          onChange={(_e, val) => onChange("channelGroup", val)}
        >
          <FormSelectOption value="stable" label="Stable" />
          <FormSelectOption value="candidate" label="Candidate" />
          <FormSelectOption value="fast" label="Fast" />
          <FormSelectOption value="eus" label="EUS" />
        </FormSelect>
      </FormGroup>
    </Form>
  );
}
