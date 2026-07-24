import {
  Form,
  FormGroup,
  FormHelperText,
  FormSelect,
  FormSelectOption,
  HelperText,
  HelperTextItem,
  TextInput,
} from "@patternfly/react-core";

import type { GcpHcpFormData } from "./CreateGcpHcpWizard";

const CLUSTER_ID_PATTERN = /^[a-z][-a-z0-9]*$/;
const CLUSTER_ID_MAX_LENGTH = 15;

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
  const trimmedId = formData.clusterId.trim();
  const idTooLong = trimmedId.length > CLUSTER_ID_MAX_LENGTH;
  const idPatternInvalid =
    trimmedId.length > 0 && !CLUSTER_ID_PATTERN.test(trimmedId);
  const idValidated = !trimmedId
    ? "default"
    : idTooLong || idPatternInvalid
      ? "error"
      : "default";

  return (
    <Form>
      <FormGroup label="Cluster ID" isRequired fieldId="cluster-id">
        <TextInput
          id="cluster-id"
          isRequired
          value={formData.clusterId}
          onChange={(_e, val) => onChange("clusterId", val)}
          placeholder="my-hcp-cluster"
          validated={idValidated}
        />
        <FormHelperText>
          <HelperText>
            <HelperTextItem
              variant={idValidated === "error" ? "error" : "default"}
            >
              {idTooLong
                ? `Must be ${CLUSTER_ID_MAX_LENGTH} characters or less (currently ${trimmedId.length}).`
                : idPatternInvalid
                  ? "Must start with a lowercase letter and contain only lowercase letters, digits, and hyphens."
                  : `Lowercase letters, digits, and hyphens. Max ${CLUSTER_ID_MAX_LENGTH} characters.`}
            </HelperTextItem>
          </HelperText>
        </FormHelperText>
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
