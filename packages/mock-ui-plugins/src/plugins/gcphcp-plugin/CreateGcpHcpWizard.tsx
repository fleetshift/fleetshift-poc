import { useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import {
  Alert,
  PageSection,
  Stack,
  StackItem,
  Title,
  Content,
  Wizard,
  WizardStep,
} from "@patternfly/react-core";
import { usePluginNavigate } from "@fleetshift/common";
import { createGcpHcpCluster } from "./api";
import ClusterDetailsStep from "./ClusterDetailsStep";
import NodePoolsStep from "./NodePoolsStep";
import ReviewStep from "./ReviewStep";

export interface NodepoolEntry {
  id: string;
  replicas: number;
  instanceType: string;
  rootVolumeSize: number;
  rootVolumeType: string;
  autoRepair: boolean;
  upgradeType: string;
}

export interface GcpHcpFormData {
  clusterId: string;
  endpointAccess: string;
  releaseVersion: string;
  channelGroup: string;
  nodepools: NodepoolEntry[];
}

const initialFormData: GcpHcpFormData = {
  clusterId: "",
  endpointAccess: "PublicAndPrivate",
  releaseVersion: "",
  channelGroup: "stable",
  nodepools: [
    {
      id: "",
      replicas: 2,
      instanceType: "n1-standard-4",
      rootVolumeSize: 128,
      rootVolumeType: "pd-standard",
      autoRepair: true,
      upgradeType: "Replace",
    },
  ],
};

interface CreateGcpHcpWizardProps {
  onSetupNext?: () => void;
  onSetupSkip?: () => void;
}

export default function CreateGcpHcpWizard({
  onSetupNext,
}: CreateGcpHcpWizardProps) {
  const navigate = useNavigate();
  const clusters = usePluginNavigate("core-plugin", "ClustersModule");
  const [formData, setFormData] = useState<GcpHcpFormData>(initialFormData);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const updateField = useCallback(
    <K extends keyof GcpHcpFormData>(field: K, value: GcpHcpFormData[K]) => {
      setFormData((prev) => ({ ...prev, [field]: value }));
    },
    [],
  );

  const handleCancel = useCallback(() => {
    navigate(-1);
  }, [navigate]);

  const handleSubmit = useCallback(async () => {
    if (
      !formData.clusterId.trim() ||
      !/^[a-z][-a-z0-9]*$/.test(formData.clusterId.trim())
    ) {
      setError(
        "Cluster ID must start with a lowercase letter and contain only lowercase letters, digits, and hyphens.",
      );
      return;
    }
    if (!formData.releaseVersion.trim()) {
      setError("Release version is required.");
      return;
    }
    if (formData.nodepools.some((np) => !np.id.trim())) {
      setError("All node pools must have an ID.");
      return;
    }

    setCreating(true);
    setError(null);

    try {
      await createGcpHcpCluster(formData.clusterId.trim(), {
        endpointAccess: formData.endpointAccess,
        releaseVersion: formData.releaseVersion.trim(),
        channelGroup: formData.channelGroup,
        nodepools: formData.nodepools.map((np) => ({
          id: np.id.trim(),
          replicas: np.replicas,
          instanceType: np.instanceType,
          rootVolumeSize: np.rootVolumeSize,
          rootVolumeType: np.rootVolumeType,
          autoRepair: np.autoRepair,
          upgradeType: np.upgradeType,
        })),
      });

      if (onSetupNext) {
        onSetupNext();
      } else {
        clusters.navigate();
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setCreating(false);
    }
  }, [formData, onSetupNext, clusters]);

  const isStep1Valid =
    /^[a-z][-a-z0-9]*$/.test(formData.clusterId.trim()) &&
    formData.releaseVersion.trim().length > 0;

  const isStep2Valid = formData.nodepools.every(
    (np) => np.id.trim().length > 0 && /^[a-z][-a-z0-9]*$/.test(np.id),
  );

  return (
    <PageSection>
      <Stack hasGutter>
        <StackItem>
          <Title headingLevel="h1" size="xl">
            Create a GCP Hosted Control Plane cluster
          </Title>
          <Content component="p">
            Configure a managed OpenShift cluster on Google Cloud Platform.
          </Content>
        </StackItem>

        {error && (
          <StackItem>
            <Alert
              variant="danger"
              title="Cluster creation failed"
              isInline
              actionClose={
                <button
                  className="pf-v6-c-alert__action-close"
                  onClick={() => setError(null)}
                />
              }
            >
              {error}
            </Alert>
          </StackItem>
        )}

        <StackItem>
          <Wizard onClose={handleCancel} height={500} isVisitRequired>
            <WizardStep
              name="Cluster details"
              id="cluster-details"
              status={isStep1Valid ? "default" : "error"}
              isDisabled={creating}
              footer={{ isNextDisabled: !isStep1Valid }}
            >
              <ClusterDetailsStep formData={formData} onChange={updateField} />
            </WizardStep>

            <WizardStep
              name="Node pools"
              id="node-pools"
              status={isStep2Valid ? "default" : "error"}
              isDisabled={creating}
              footer={{ isNextDisabled: !isStep2Valid }}
            >
              <NodePoolsStep formData={formData} onChange={updateField} />
            </WizardStep>

            <WizardStep
              name="Review"
              id="review"
              isDisabled={creating}
              footer={{
                nextButtonText: creating ? "Creating..." : "Create cluster",
                onNext: handleSubmit,
                isNextDisabled: creating,
              }}
            >
              <ReviewStep formData={formData} />
            </WizardStep>
          </Wizard>
        </StackItem>
      </Stack>
    </PageSection>
  );
}
