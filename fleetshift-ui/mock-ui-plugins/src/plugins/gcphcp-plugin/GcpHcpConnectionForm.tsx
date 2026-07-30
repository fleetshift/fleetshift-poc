import type { OnboardingActionFormProps } from "@fleetshift/common";
import {
  Button,
  Form,
  FormGroup,
  FormSection,
  MenuToggle,
  Select,
  SelectList,
  SelectOption,
  TextInput,
  Title,
} from "@patternfly/react-core";
import { useCallback, useState } from "react";

import ConnectionProgress from "./ConnectionProgress";

const GCP_REGIONS = [
  "us-central1",
  "us-east1",
  "us-east4",
  "us-west1",
  "us-west2",
  "europe-west1",
  "europe-west4",
  "asia-east1",
  "asia-southeast1",
];

interface FormState {
  projectId: string;
  region: string;
  gatewayUrl: string;
  gatewayAudience: string;
  workforcePool: string;
  workforceProvider: string;
  brokerSaEmail: string;
  targetId: string;
}

const INITIAL_STATE: FormState = {
  projectId: "",
  region: GCP_REGIONS[0],
  gatewayUrl: "",
  gatewayAudience: "",
  workforcePool: "",
  workforceProvider: "",
  brokerSaEmail: "",
  targetId: "",
};

export default function GcpHcpConnectionForm({
  onComplete,
  onCancel,
}: OnboardingActionFormProps) {
  const [form, setForm] = useState<FormState>(INITIAL_STATE);
  const [regionOpen, setRegionOpen] = useState(false);
  const [connecting, setConnecting] = useState(false);

  const update = useCallback(
    <K extends keyof FormState>(field: K, value: FormState[K]) => {
      setForm((prev) => ({ ...prev, [field]: value }));
    },
    [],
  );

  const isValid =
    form.projectId.trim() !== "" &&
    form.gatewayUrl.trim() !== "" &&
    form.gatewayAudience.trim() !== "" &&
    form.workforcePool.trim() !== "" &&
    form.workforceProvider.trim() !== "" &&
    form.brokerSaEmail.trim() !== "" &&
    form.targetId.trim() !== "";

  if (connecting) {
    return <ConnectionProgress onComplete={onComplete} />;
  }

  return (
    <>
      <div className="ome-setup-whats-next__body">
        <div className="ome-setup-whats-next__inner">
          <Title headingLevel="h1" className="ome-setup-whats-next__title">
            GCP Hosted Control Plane
          </Title>
          <Form>
            <FormSection title="GCP Project">
              <FormGroup label="Project ID" isRequired fieldId="gcp-project-id">
                <TextInput
                  id="gcp-project-id"
                  isRequired
                  value={form.projectId}
                  onChange={(_e, v) => update("projectId", v)}
                  placeholder="my-gcp-project"
                />
              </FormGroup>
              <FormGroup label="Region" isRequired fieldId="gcp-region">
                <Select
                  id="gcp-region"
                  isOpen={regionOpen}
                  onOpenChange={setRegionOpen}
                  onSelect={(_e, val) => {
                    update("region", val as string);
                    setRegionOpen(false);
                  }}
                  selected={form.region}
                  toggle={(toggleRef) => (
                    <MenuToggle
                      ref={toggleRef}
                      onClick={() => setRegionOpen((prev) => !prev)}
                      isExpanded={regionOpen}
                      isFullWidth
                    >
                      {form.region}
                    </MenuToggle>
                  )}
                >
                  <SelectList>
                    {GCP_REGIONS.map((r) => (
                      <SelectOption key={r} value={r}>
                        {r}
                      </SelectOption>
                    ))}
                  </SelectList>
                </Select>
              </FormGroup>
            </FormSection>

            <FormSection title="Workforce Identity">
              <FormGroup
                label="Workforce Pool"
                isRequired
                fieldId="gcp-workforce-pool"
              >
                <TextInput
                  id="gcp-workforce-pool"
                  isRequired
                  value={form.workforcePool}
                  onChange={(_e, v) => update("workforcePool", v)}
                  placeholder="locations/global/workforcePools/my-pool"
                />
              </FormGroup>
              <FormGroup
                label="Workforce Provider"
                isRequired
                fieldId="gcp-workforce-provider"
              >
                <TextInput
                  id="gcp-workforce-provider"
                  isRequired
                  value={form.workforceProvider}
                  onChange={(_e, v) => update("workforceProvider", v)}
                  placeholder="locations/global/workforcePools/my-pool/providers/my-provider"
                />
              </FormGroup>
              <FormGroup
                label="Broker Service Account Email"
                isRequired
                fieldId="gcp-broker-sa"
              >
                <TextInput
                  id="gcp-broker-sa"
                  isRequired
                  value={form.brokerSaEmail}
                  onChange={(_e, v) => update("brokerSaEmail", v)}
                  placeholder="broker@my-project.iam.gserviceaccount.com"
                />
              </FormGroup>
            </FormSection>

            <FormSection title="Gateway">
              <FormGroup
                label="Gateway URL"
                isRequired
                fieldId="gcp-gateway-url"
              >
                <TextInput
                  id="gcp-gateway-url"
                  isRequired
                  value={form.gatewayUrl}
                  onChange={(_e, v) => update("gatewayUrl", v)}
                  placeholder="https://gateway.example.com"
                />
              </FormGroup>
              <FormGroup
                label="Gateway Audience"
                isRequired
                fieldId="gcp-gateway-audience"
              >
                <TextInput
                  id="gcp-gateway-audience"
                  isRequired
                  value={form.gatewayAudience}
                  onChange={(_e, v) => update("gatewayAudience", v)}
                  placeholder="https://gateway.example.com"
                />
              </FormGroup>
            </FormSection>

            <FormSection title="FleetShift Target">
              <FormGroup label="Target ID" isRequired fieldId="gcp-target-id">
                <TextInput
                  id="gcp-target-id"
                  isRequired
                  value={form.targetId}
                  onChange={(_e, v) => update("targetId", v)}
                  placeholder="my-target"
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
