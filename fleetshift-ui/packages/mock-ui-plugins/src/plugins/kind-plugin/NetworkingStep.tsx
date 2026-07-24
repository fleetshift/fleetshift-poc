import { Form, FormGroup, TextInput } from "@patternfly/react-core";

import type { ClusterFormData } from "./CreateClusterWizard";

interface NetworkingStepProps {
  formData: ClusterFormData;
  onChange: <K extends keyof ClusterFormData>(
    field: K,
    value: ClusterFormData[K],
  ) => void;
}

export default function NetworkingStep({
  formData,
  onChange,
}: NetworkingStepProps) {
  return (
    <Form>
      <FormGroup label="API server port" fieldId="api-server-port">
        <TextInput
          id="api-server-port"
          type="number"
          value={formData.apiServerPort || ""}
          onChange={(_e, val) => onChange("apiServerPort", Number(val) || 0)}
          placeholder="0 (auto)"
        />
      </FormGroup>

      <FormGroup label="Pod subnet" fieldId="pod-subnet">
        <TextInput
          id="pod-subnet"
          value={formData.podSubnet}
          onChange={(_e, val) => onChange("podSubnet", val)}
          placeholder="10.244.0.0/16"
        />
      </FormGroup>

      <FormGroup label="Service subnet" fieldId="service-subnet">
        <TextInput
          id="service-subnet"
          value={formData.serviceSubnet}
          onChange={(_e, val) => onChange("serviceSubnet", val)}
          placeholder="10.96.0.0/16"
        />
      </FormGroup>
    </Form>
  );
}
