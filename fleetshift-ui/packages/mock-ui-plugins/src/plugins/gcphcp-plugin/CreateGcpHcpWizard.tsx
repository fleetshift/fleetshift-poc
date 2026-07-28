import FormTemplate from "@data-driven-forms/pf4-component-mapper/form-template";
import TextField from "@data-driven-forms/pf4-component-mapper/text-field";
import DDFWizard from "@data-driven-forms/pf4-component-mapper/wizard";
import {
  componentTypes,
  FormRenderer,
  Schema,
} from "@data-driven-forms/react-form-renderer";
import type { ClusterProviderWizardProps } from "@fleetshift/common";
import { usePluginNavigate } from "@fleetshift/common";
import { Alert } from "@patternfly/react-core";
import type { ReactNode } from "react";
import { useCallback, useState } from "react";

import { createGcpHcpCluster } from "./api";
import DdfFormSelect from "./ddfComponents/DdfFormSelect";
import DdfNodePoolsStep from "./ddfComponents/DdfNodePoolsStep";
import DdfReviewStep from "./ddfComponents/DdfReviewStep";
import { clusterIdValidator } from "./formValidators/formValidators";
import { DEFAULT_NODEPOOL } from "./nodePoolYaml";

const FORM_SELECT = "form-select";
const NODE_POOLS_STEP = "node-pools-step";
const REVIEW_STEP = "review-step";

const componentMapper = {
  [componentTypes.WIZARD]: DDFWizard,
  [componentTypes.TEXT_FIELD]: TextField,
  [FORM_SELECT]: DdfFormSelect,
  [NODE_POOLS_STEP]: DdfNodePoolsStep,
  [REVIEW_STEP]: DdfReviewStep,
};

const FormTemplateWrapper = (props: Record<string, unknown>) => (
  <FormTemplate {...props} showFormControls={false} />
);

// DDF pf4-component-mapper wizard step uses "pf-c-form" (PF4).
// Use a div with PF6 form class for spacing without nesting <form> inside <form>.
function Pf6StepTemplate({ formFields }: { formFields: ReactNode }) {
  return <div className="pf-v6-c-form">{formFields}</div>;
}

const formSchema: Schema = {
  fields: [
    {
      // title: "Create GCP HCP Cluster",
      name: "gcp-hcp-wizard",
      component: componentTypes.WIZARD,
      inModal: false,
      fields: [
        {
          title: "Cluster details",
          name: "cluster-details",
          nextStep: "node-pools",
          StepTemplate: Pf6StepTemplate,
          component: "",
          fields: [
            {
              name: "clusterId",
              isRequired: true,
              component: componentTypes.TEXT_FIELD,
              label: "Cluster ID",
              validate: [clusterIdValidator],
              helperText:
                "Lowercase letters, digits, and hyphens. Max 15 characters.",
              placeholder: "my-hcp-cluster",
            },
            {
              name: "endpointAccess",
              component: FORM_SELECT,
              label: "Endpoint access",
              isRequired: true,
              validate: [{ type: "required" }],
              options: [
                { label: "Public and Private", value: "PublicAndPrivate" },
                { label: "Public", value: "Public" },
                { label: "Private", value: "Private" },
              ],
            },
            {
              name: "releaseVersion",
              isRequired: true,
              component: componentTypes.TEXT_FIELD,
              label: "Release version",
              validate: [{ type: "required" }],
              placeholder: "4.22.0",
            },
            {
              name: "channelGroup",
              component: FORM_SELECT,
              label: "Channel group",
              isRequired: true,
              validate: [{ type: "required" }],
              options: [
                { label: "Stable", value: "stable" },
                { label: "Candidate", value: "candidate" },
                { label: "Fast", value: "fast" },
                { label: "EUS", value: "eus" },
              ],
            },
          ],
        },
        {
          title: "Node pools",
          name: "node-pools",
          nextStep: "review",
          StepTemplate: Pf6StepTemplate,
          component: "",
          fields: [
            {
              name: "nodepools",
              component: NODE_POOLS_STEP,
            },
          ],
        },
        {
          title: "Review",
          name: "review",
          StepTemplate: Pf6StepTemplate,
          component: "",
          fields: [
            {
              name: "review-content",
              component: REVIEW_STEP,
            },
          ],
        },
      ],
    },
  ],
};

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

const initialValues: GcpHcpFormData = {
  clusterId: "",
  endpointAccess: "PublicAndPrivate",
  releaseVersion: "",
  channelGroup: "stable",
  nodepools: [{ ...DEFAULT_NODEPOOL }],
};

export default function CreateGcpHcpWizard({
  onClose,
  onSetupNext,
}: ClusterProviderWizardProps) {
  const clusters = usePluginNavigate("core-plugin", "ClustersModule");
  const [error, setError] = useState<string | null>(null);

  const handleCancel = useCallback(() => {
    if (onClose) {
      onClose();
    } else {
      clusters.navigate();
    }
  }, [onClose, clusters]);

  const handleSubmit = useCallback(
    async (values: GcpHcpFormData) => {
      setError(null);

      try {
        await createGcpHcpCluster(values.clusterId.trim(), {
          endpointAccess: values.endpointAccess,
          releaseVersion: values.releaseVersion.trim(),
          channelGroup: values.channelGroup,
          nodepools: values.nodepools.map((np) => ({
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
        } else if (onClose) {
          onClose();
        } else {
          clusters.navigate();
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [onSetupNext, onClose, clusters],
  );

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
      <FormRenderer
        schema={formSchema}
        componentMapper={componentMapper}
        FormTemplate={FormTemplateWrapper}
        initialValues={initialValues}
        onSubmit={(values) => handleSubmit(values as GcpHcpFormData)}
        onCancel={handleCancel}
      />
    </>
  );
}
