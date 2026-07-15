import type { OnboardingActionFormProps } from "@fleetshift/common";
import {
  Button,
  Form,
  FormGroup,
  FormSection,
  TextInput,
  Title,
} from "@patternfly/react-core";
import { useCallback, useState } from "react";

import kindLogo from "../../kind-plugin/assets/kind-logo.png";
import { createOnboardingCard } from "../shared/GenericOnboardingCard";
import KindConnectionProgress from "./KindConnectionProgress";

function KindOnboardingIcon() {
  return <img src={kindLogo} alt="Kind" width="100%" height="100%" />;
}

export const KindOnboardingCard = createOnboardingCard({
  title: "Set up Kind",
  description: "Configure a local Kind cluster for development and testing.",
  icon: KindOnboardingIcon,
});

interface FormState {
  clusterName: string;
  nodeImage: string;
  apiServerPort: string;
}

const INITIAL_STATE: FormState = {
  clusterName: "",
  nodeImage: "kindest/node:v1.31.0",
  apiServerPort: "0",
};

export function KindOnboardingForm({
  onComplete,
  onCancel,
}: OnboardingActionFormProps) {
  const [form, setForm] = useState<FormState>(INITIAL_STATE);
  const [connecting, setConnecting] = useState(false);

  const update = useCallback(
    <K extends keyof FormState>(field: K, value: FormState[K]) => {
      setForm((prev) => ({ ...prev, [field]: value }));
    },
    [],
  );

  const isValid = form.clusterName.trim() !== "";

  if (connecting) {
    return <KindConnectionProgress onComplete={onComplete} />;
  }

  return (
    <>
      <div className="ome-setup-whats-next__body">
        <div className="ome-setup-whats-next__inner">
          <Title headingLevel="h1" className="ome-setup-whats-next__title">
            Kind Local Cluster
          </Title>
          <Form>
            <FormSection title="Cluster Details">
              <FormGroup
                label="Cluster name"
                isRequired
                fieldId="kind-cluster-name"
              >
                <TextInput
                  id="kind-cluster-name"
                  isRequired
                  value={form.clusterName}
                  onChange={(_e, v) => update("clusterName", v)}
                  placeholder="my-cluster"
                />
              </FormGroup>
              <FormGroup label="Node image" fieldId="kind-node-image">
                <TextInput
                  id="kind-node-image"
                  value={form.nodeImage}
                  onChange={(_e, v) => update("nodeImage", v)}
                  placeholder="kindest/node:v1.31.0"
                />
              </FormGroup>
              <FormGroup
                label="API server port"
                fieldId="kind-api-port"
                helperText="Leave 0 for auto-assign"
              >
                <TextInput
                  id="kind-api-port"
                  type="number"
                  value={form.apiServerPort}
                  onChange={(_e, v) => update("apiServerPort", v)}
                />
              </FormGroup>
            </FormSection>
          </Form>
        </div>
      </div>
      <div className="ome-setup-whats-next__toolbar">
        <div className="ome-setup-whats-next__toolbar-inner">
          <Button
            variant="primary"
            isDisabled={!isValid}
            onClick={() => setConnecting(true)}
          >
            Connect
          </Button>
          <Button variant="link" onClick={onCancel}>
            Cancel
          </Button>
        </div>
      </div>
    </>
  );
}
