import type { ClusterProviderWizardProps } from "@fleetshift/common";
import { usePluginNavigate } from "@fleetshift/common";
import { Alert, Wizard, WizardStep } from "@patternfly/react-core";
import { useCallback, useState } from "react";

import {
  createKindCluster,
  type KindClusterSpec,
  type KindNetworkingSpec,
} from "./api";
import ClusterDetailsStep from "./ClusterDetailsStep";
import NetworkingStep from "./NetworkingStep";
import NodesStep from "./NodesStep";
import ReviewStep from "./ReviewStep";

export interface NodeEntry {
  role: "control-plane" | "worker";
  image: string;
}

export interface ClusterFormData {
  name: string;
  nodeImage: string;
  apiServerPort: number;
  podSubnet: string;
  serviceSubnet: string;
  nodes: NodeEntry[];
}

const initialFormData: ClusterFormData = {
  name: "",
  nodeImage: "",
  apiServerPort: 0,
  podSubnet: "",
  serviceSubnet: "",
  nodes: [{ role: "control-plane", image: "" }],
};

export default function CreateClusterWizard({
  onClose,
  onSetupNext,
}: ClusterProviderWizardProps) {
  const clusters = usePluginNavigate("core-plugin", "ClustersModule");
  const [formData, setFormData] = useState<ClusterFormData>(initialFormData);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const updateField = useCallback(
    <K extends keyof ClusterFormData>(field: K, value: ClusterFormData[K]) => {
      setFormData((prev) => ({ ...prev, [field]: value }));
    },
    [],
  );

  const handleCancel = useCallback(() => {
    if (onClose) {
      onClose();
    } else {
      clusters.navigate();
    }
  }, [onClose, clusters]);

  const handleSubmit = useCallback(async () => {
    if (!formData.name.trim()) {
      setError("Cluster name is required.");
      return;
    }

    setCreating(true);
    setError(null);

    try {
      const spec: KindClusterSpec = {};

      const nodes = formData.nodes
        .filter((n) => n.role)
        .map((n) => ({
          role: n.role as "control-plane" | "worker",
          ...(n.image || formData.nodeImage
            ? { image: n.image || formData.nodeImage }
            : {}),
        }));
      if (nodes.length > 0) {
        spec.nodes = nodes;
      }

      const networking: KindNetworkingSpec = {};
      if (formData.apiServerPort > 0) {
        networking.apiServerPort = formData.apiServerPort;
      }
      if (formData.podSubnet.trim()) {
        networking.podSubnet = formData.podSubnet.trim();
      }
      if (formData.serviceSubnet.trim()) {
        networking.serviceSubnet = formData.serviceSubnet.trim();
      }
      if (Object.keys(networking).length > 0) {
        spec.networking = networking;
      }

      await createKindCluster(formData.name.trim(), spec);

      if (onSetupNext) {
        onSetupNext();
      } else if (onClose) {
        onClose();
      } else {
        clusters.navigate(formData.name.trim());
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setCreating(false);
    }
  }, [formData, onSetupNext, onClose, clusters]);

  const isStep1Valid = formData.name.trim().length > 0;

  return (
    <>
      {error && (
        <Alert
          variant="danger"
          title="Cluster creation failed"
          isInline
          className="pf-v6-u-mb-md"
          actionClose={
            <button
              className="pf-v6-c-alert__action-close"
              onClick={() => setError(null)}
            />
          }
        >
          {error}
        </Alert>
      )}
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

        <WizardStep name="Networking" id="networking" isDisabled={creating}>
          <NetworkingStep formData={formData} onChange={updateField} />
        </WizardStep>

        <WizardStep name="Nodes" id="nodes" isDisabled={creating}>
          <NodesStep formData={formData} onChange={updateField} />
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
    </>
  );
}
