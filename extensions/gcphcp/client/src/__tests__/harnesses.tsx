import FormTemplate from "@data-driven-forms/pf4-component-mapper/form-template";
import TextField from "@data-driven-forms/pf4-component-mapper/text-field";
import {
  componentTypes,
  FormRenderer,
} from "@data-driven-forms/react-form-renderer";
import { useState } from "react";

import type { GcpHcpFormData, NodepoolEntry } from "../CreateGcpHcpWizard";
import DdfFormSelect from "../ddfComponents/DdfFormSelect";
import { clusterIdValidator } from "../formValidators/formValidators";
import NodePoolsStep from "../NodePoolsStep";
import { DEFAULT_NODEPOOL } from "../nodePoolYaml";

const FORM_SELECT = "form-select";

const componentMapper = {
  [componentTypes.TEXT_FIELD]: TextField,
  [FORM_SELECT]: DdfFormSelect,
};

const FormTemplateNoControls = (props: Record<string, unknown>) => (
  <FormTemplate {...props} showFormControls={false} />
);

// --- DdfFormSelect harnesses ---

export function SelectBasic() {
  return (
    <FormRenderer
      schema={{
        fields: [
          {
            name: "color",
            component: FORM_SELECT,
            label: "Favorite color",
            isRequired: true,
            options: [
              { label: "Red", value: "red" },
              { label: "Blue", value: "blue" },
              { label: "Green", value: "green" },
            ],
          },
        ],
      }}
      componentMapper={componentMapper}
      FormTemplate={FormTemplateNoControls}
      onSubmit={() => {}}
      initialValues={{}}
    />
  );
}

export function SelectWithHelperText() {
  return (
    <FormRenderer
      schema={{
        fields: [
          {
            name: "color",
            component: FORM_SELECT,
            label: "Favorite color",
            isRequired: true,
            helperText: "Pick your favorite",
            options: [
              { label: "Red", value: "red" },
              { label: "Blue", value: "blue" },
            ],
          },
        ],
      }}
      componentMapper={componentMapper}
      FormTemplate={FormTemplateNoControls}
      onSubmit={() => {}}
      initialValues={{}}
    />
  );
}

export function SelectWithRequired() {
  return (
    <FormRenderer
      schema={{
        fields: [
          {
            name: "color",
            component: FORM_SELECT,
            label: "Color",
            isRequired: true,
            validate: [{ type: "required" }],
            options: [
              { label: "-- Select --", value: "" },
              { label: "Red", value: "red" },
            ],
          },
        ],
      }}
      componentMapper={componentMapper}
      FormTemplate={FormTemplateNoControls}
      onSubmit={() => {}}
      initialValues={{ color: "" }}
    />
  );
}

export function SelectWithInitialValue() {
  return (
    <FormRenderer
      schema={{
        fields: [
          {
            name: "color",
            component: FORM_SELECT,
            label: "Color",
            options: [
              { label: "Red", value: "red" },
              { label: "Blue", value: "blue" },
            ],
          },
        ],
      }}
      componentMapper={componentMapper}
      FormTemplate={FormTemplateNoControls}
      onSubmit={() => {}}
      initialValues={{ color: "blue" }}
    />
  );
}

// --- ClusterDetails step 1 harness ---

export function ClusterDetailsStep1() {
  return (
    <FormRenderer
      schema={{
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
      }}
      componentMapper={componentMapper}
      FormTemplate={FormTemplateNoControls}
      onSubmit={() => {}}
      initialValues={{
        clusterId: "",
        endpointAccess: "PublicAndPrivate",
        releaseVersion: "",
        channelGroup: "stable",
      }}
    />
  );
}

// --- NodePoolsStep harnesses ---

export function NodePoolsDefault() {
  const [formData, setFormData] = useState<GcpHcpFormData>({
    clusterId: "",
    endpointAccess: "PublicAndPrivate",
    releaseVersion: "",
    channelGroup: "stable",
    nodepools: [{ ...DEFAULT_NODEPOOL }],
  });

  const onChange = <K extends keyof GcpHcpFormData>(
    field: K,
    value: GcpHcpFormData[K],
  ) => {
    setFormData((prev) => ({ ...prev, [field]: value }));
  };

  return <NodePoolsStep formData={formData} onChange={onChange} />;
}

export function NodePoolsTwoPools() {
  const pool1: NodepoolEntry = { ...DEFAULT_NODEPOOL, id: "pool-a" };
  const pool2: NodepoolEntry = { ...DEFAULT_NODEPOOL, id: "pool-b" };

  const [formData, setFormData] = useState<GcpHcpFormData>({
    clusterId: "",
    endpointAccess: "PublicAndPrivate",
    releaseVersion: "",
    channelGroup: "stable",
    nodepools: [pool1, pool2],
  });

  const onChange = <K extends keyof GcpHcpFormData>(
    field: K,
    value: GcpHcpFormData[K],
  ) => {
    setFormData((prev) => ({ ...prev, [field]: value }));
  };

  return <NodePoolsStep formData={formData} onChange={onChange} />;
}
