import "./NodesStep.scss";

import {
  Button,
  Form,
  FormGroup,
  FormSelect,
  FormSelectOption,
  TextInput,
} from "@patternfly/react-core";
import PlusCircleIcon from "@patternfly/react-icons/dist/dynamic/icons/plus-circle-icon";
import TrashIcon from "@patternfly/react-icons/dist/dynamic/icons/trash-icon";

import type { ClusterFormData, NodeEntry } from "./CreateClusterWizard";

interface NodesStepProps {
  formData: ClusterFormData;
  onChange: <K extends keyof ClusterFormData>(
    field: K,
    value: ClusterFormData[K],
  ) => void;
}

export default function NodesStep({ formData, onChange }: NodesStepProps) {
  const updateNode = (index: number, patch: Partial<NodeEntry>) => {
    const updated = formData.nodes.map((n, i) =>
      i === index ? { ...n, ...patch } : n,
    );
    onChange("nodes", updated);
  };

  const addNode = () => {
    onChange("nodes", [...formData.nodes, { role: "worker", image: "" }]);
  };

  const removeNode = (index: number) => {
    if (formData.nodes.length <= 1) return;
    onChange(
      "nodes",
      formData.nodes.filter((_, i) => i !== index),
    );
  };

  return (
    <Form>
      {formData.nodes.map((node, i) => (
        <div
          key={i}
          className="pf-v6-u-display-flex pf-v6-u-align-items-flex-end ome-kind-node-row"
        >
          <FormGroup
            label={i === 0 ? "Role" : undefined}
            fieldId={`node-role-${i}`}
            className="pf-v6-u-flex-1"
          >
            <FormSelect
              id={`node-role-${i}`}
              value={node.role}
              onChange={(_e, val) =>
                updateNode(i, {
                  role: val as "control-plane" | "worker",
                })
              }
            >
              <FormSelectOption value="control-plane" label="Control Plane" />
              <FormSelectOption value="worker" label="Worker" />
            </FormSelect>
          </FormGroup>

          <FormGroup
            label={i === 0 ? "Image (optional)" : undefined}
            fieldId={`node-image-${i}`}
            className="ome-kind-node-image"
          >
            <TextInput
              id={`node-image-${i}`}
              value={node.image}
              onChange={(_e, val) => updateNode(i, { image: val })}
              placeholder={formData.nodeImage || "kindest/node (default)"}
            />
          </FormGroup>

          <Button
            variant="plain"
            aria-label="Remove node"
            onClick={() => removeNode(i)}
            isDisabled={formData.nodes.length <= 1}
          >
            <TrashIcon />
          </Button>
        </div>
      ))}

      <Button variant="link" icon={<PlusCircleIcon />} onClick={addNode}>
        Add node
      </Button>
    </Form>
  );
}
