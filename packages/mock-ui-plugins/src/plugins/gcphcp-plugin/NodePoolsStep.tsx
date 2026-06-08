import { useState, useCallback, useEffect } from "react";
import {
  Button,
  Checkbox,
  Flex,
  FlexItem,
  Form,
  FormFieldGroupExpandable,
  FormFieldGroupHeader,
  FormGroup,
  FormSelect,
  FormSelectOption,
  Grid,
  GridItem,
  HelperText,
  HelperTextItem,
  NumberInput,
  TextInput,
  ToggleGroup,
  ToggleGroupItem,
} from "@patternfly/react-core";
import { CodeEditor, Language } from "@patternfly/react-code-editor";
import TrashIcon from "@patternfly/react-icons/dist/dynamic/icons/trash-icon";
import PlusCircleIcon from "@patternfly/react-icons/dist/dynamic/icons/plus-circle-icon";
import type { GcpHcpFormData, NodepoolEntry } from "./CreateGcpHcpWizard";

interface NodePoolsStepProps {
  formData: GcpHcpFormData;
  onChange: <K extends keyof GcpHcpFormData>(
    field: K,
    value: GcpHcpFormData[K],
  ) => void;
}

const DEFAULT_NODEPOOL: NodepoolEntry = {
  id: "",
  replicas: 2,
  instanceType: "n1-standard-4",
  rootVolumeSize: 128,
  rootVolumeType: "pd-standard",
  autoRepair: true,
  upgradeType: "Replace",
};

function poolSummary(pool: NodepoolEntry): string {
  return `${pool.replicas}x ${pool.instanceType}, ${pool.rootVolumeSize}GB ${pool.rootVolumeType}`;
}

function serializeToYaml(pools: NodepoolEntry[]): string {
  return pools
    .map((p) =>
      [
        `- id: ${p.id || '""'}`,
        `  replicas: ${p.replicas}`,
        `  instanceType: ${p.instanceType}`,
        `  rootVolumeSize: ${p.rootVolumeSize}`,
        `  rootVolumeType: ${p.rootVolumeType}`,
        `  autoRepair: ${p.autoRepair}`,
        `  upgradeType: ${p.upgradeType}`,
      ].join("\n"),
    )
    .join("\n");
}

function parseFromYaml(text: string): NodepoolEntry[] | null {
  try {
    const pools: NodepoolEntry[] = [];
    let current: Partial<NodepoolEntry> | null = null;

    for (const rawLine of text.split("\n")) {
      const line = rawLine.trimEnd();
      if (!line.trim()) continue;

      if (line.startsWith("- ")) {
        if (current) pools.push(finishPool(current));
        current = {};
        parseField(current, line.slice(2));
      } else if (line.startsWith("  ") && current) {
        parseField(current, line.trim());
      } else {
        return null;
      }
    }
    if (current) pools.push(finishPool(current));
    return pools.length > 0 ? pools : null;
  } catch {
    return null;
  }
}

function parseField(pool: Partial<NodepoolEntry>, field: string) {
  const colonIdx = field.indexOf(":");
  if (colonIdx === -1) return;
  const key = field.slice(0, colonIdx).trim();
  let val = field.slice(colonIdx + 1).trim();

  if (val.startsWith('"') && val.endsWith('"')) {
    val = val.slice(1, -1);
  }

  switch (key) {
    case "id":
      pool.id = val;
      break;
    case "replicas":
      pool.replicas = parseInt(val, 10) || 1;
      break;
    case "instanceType":
      pool.instanceType = val;
      break;
    case "rootVolumeSize":
      pool.rootVolumeSize = parseInt(val, 10) || 128;
      break;
    case "rootVolumeType":
      pool.rootVolumeType = val;
      break;
    case "autoRepair":
      pool.autoRepair = val === "true";
      break;
    case "upgradeType":
      pool.upgradeType = val;
      break;
  }
}

function finishPool(partial: Partial<NodepoolEntry>): NodepoolEntry {
  return { ...DEFAULT_NODEPOOL, ...partial };
}

const VALID_INSTANCE_TYPES = new Set([
  "n1-standard-4",
  "n1-standard-8",
  "n1-standard-16",
  "n2-standard-4",
  "n2-standard-8",
]);
const VALID_VOLUME_TYPES = new Set(["pd-standard", "pd-ssd"]);
const VALID_UPGRADE_TYPES = new Set(["Replace", "InPlace"]);

function validatePools(pools: NodepoolEntry[]): string[] {
  const errors: string[] = [];
  const ids = new Set<string>();

  pools.forEach((pool, i) => {
    const label = pool.id || `pool ${i + 1}`;

    if (pool.id && !/^[a-z][-a-z0-9]*$/.test(pool.id)) {
      errors.push(
        `${label}: ID must start with a lowercase letter and contain only a-z, 0-9, hyphens`,
      );
    }

    if (pool.id && ids.has(pool.id)) {
      errors.push(`${label}: duplicate pool ID`);
    }
    if (pool.id) ids.add(pool.id);

    if (pool.replicas < 1) {
      errors.push(`${label}: replicas must be at least 1`);
    }

    if (!VALID_INSTANCE_TYPES.has(pool.instanceType)) {
      errors.push(`${label}: unknown instance type "${pool.instanceType}"`);
    }

    if (pool.rootVolumeSize < 1) {
      errors.push(`${label}: root volume size must be at least 1`);
    }

    if (!VALID_VOLUME_TYPES.has(pool.rootVolumeType)) {
      errors.push(`${label}: unknown volume type "${pool.rootVolumeType}"`);
    }

    if (!VALID_UPGRADE_TYPES.has(pool.upgradeType)) {
      errors.push(`${label}: upgrade type must be "Replace" or "InPlace"`);
    }
  });

  return errors;
}

export default function NodePoolsStep({
  formData,
  onChange,
}: NodePoolsStepProps) {
  const [viewMode, setViewMode] = useState<"form" | "yaml">("form");
  const [yamlText, setYamlText] = useState(() =>
    serializeToYaml(formData.nodepools),
  );
  const [yamlValid, setYamlValid] = useState(true);
  const [yamlErrors, setYamlErrors] = useState<string[]>([]);

  useEffect(() => {
    if (viewMode === "form") {
      setYamlText(serializeToYaml(formData.nodepools));
      setYamlValid(true);
    }
  }, [formData.nodepools, viewMode]);

  const handleYamlChange = useCallback(
    (val: string) => {
      setYamlText(val);
      const parsed = parseFromYaml(val);
      if (parsed) {
        const errors = validatePools(parsed);
        setYamlErrors(errors);
        setYamlValid(true);
        onChange("nodepools", parsed);
      } else {
        setYamlErrors([]);
        setYamlValid(false);
      }
    },
    [onChange],
  );

  const updatePool = (index: number, patch: Partial<NodepoolEntry>) => {
    const updated = formData.nodepools.map((p, i) =>
      i === index ? { ...p, ...patch } : p,
    );
    onChange("nodepools", updated);
  };

  const addPool = () => {
    onChange("nodepools", [...formData.nodepools, { ...DEFAULT_NODEPOOL }]);
  };

  const removePool = (index: number) => {
    if (formData.nodepools.length <= 1) return;
    onChange(
      "nodepools",
      formData.nodepools.filter((_, i) => i !== index),
    );
  };

  return (
    <>
      <Flex justifyContent={{ default: "justifyContentFlexEnd" }}>
        <FlexItem>
          <ToggleGroup isCompact aria-label="View mode">
            <ToggleGroupItem
              text="Form"
              isSelected={viewMode === "form"}
              onChange={() => setViewMode("form")}
            />
            <ToggleGroupItem
              text="YAML"
              isSelected={viewMode === "yaml"}
              onChange={() => setViewMode("yaml")}
            />
          </ToggleGroup>
        </FlexItem>
      </Flex>

      {viewMode === "form" && (
        <Form>
          {formData.nodepools.map((pool, i) => (
            <FormFieldGroupExpandable
              key={i}
              isExpanded={i === 0}
              header={
                <FormFieldGroupHeader
                  titleText={{
                    text: pool.id || `Node pool ${i + 1}`,
                    id: `pool-title-${i}`,
                  }}
                  titleDescription={poolSummary(pool)}
                  actions={
                    <Button
                      variant="plain"
                      aria-label="Remove node pool"
                      icon={<TrashIcon />}
                      onClick={() => removePool(i)}
                      isDisabled={formData.nodepools.length <= 1}
                    />
                  }
                />
              }
            >
              <Grid hasGutter>
                <GridItem span={6}>
                  <FormGroup
                    label="Pool ID"
                    isRequired
                    fieldId={`pool-id-${i}`}
                  >
                    <TextInput
                      id={`pool-id-${i}`}
                      isRequired
                      value={pool.id}
                      onChange={(_e, val) => updatePool(i, { id: val })}
                      placeholder="workers"
                      validated={
                        pool.id.trim() && /^[a-z][-a-z0-9]*$/.test(pool.id)
                          ? "default"
                          : "error"
                      }
                    />
                  </FormGroup>
                </GridItem>
                <GridItem span={6}>
                  <FormGroup
                    label="Replicas"
                    isRequired
                    fieldId={`replicas-${i}`}
                  >
                    <NumberInput
                      id={`replicas-${i}`}
                      value={pool.replicas}
                      min={1}
                      onMinus={() =>
                        updatePool(i, {
                          replicas: Math.max(1, pool.replicas - 1),
                        })
                      }
                      onPlus={() =>
                        updatePool(i, { replicas: pool.replicas + 1 })
                      }
                      onChange={(e) => {
                        const val = Number(
                          (e.target as HTMLInputElement).value,
                        );
                        if (!isNaN(val) && val >= 1)
                          updatePool(i, { replicas: val });
                      }}
                    />
                  </FormGroup>
                </GridItem>

                <GridItem span={6}>
                  <FormGroup
                    label="Instance type"
                    isRequired
                    fieldId={`instance-type-${i}`}
                  >
                    <FormSelect
                      id={`instance-type-${i}`}
                      value={pool.instanceType}
                      onChange={(_e, val) =>
                        updatePool(i, { instanceType: val })
                      }
                    >
                      <FormSelectOption
                        value="n1-standard-4"
                        label="n1-standard-4"
                      />
                      <FormSelectOption
                        value="n1-standard-8"
                        label="n1-standard-8"
                      />
                      <FormSelectOption
                        value="n1-standard-16"
                        label="n1-standard-16"
                      />
                      <FormSelectOption
                        value="n2-standard-4"
                        label="n2-standard-4"
                      />
                      <FormSelectOption
                        value="n2-standard-8"
                        label="n2-standard-8"
                      />
                    </FormSelect>
                  </FormGroup>
                </GridItem>
                <GridItem span={6}>
                  <FormGroup
                    label="Root volume size (GB)"
                    isRequired
                    fieldId={`volume-size-${i}`}
                  >
                    <NumberInput
                      id={`volume-size-${i}`}
                      value={pool.rootVolumeSize}
                      min={1}
                      onMinus={() =>
                        updatePool(i, {
                          rootVolumeSize: Math.max(1, pool.rootVolumeSize - 1),
                        })
                      }
                      onPlus={() =>
                        updatePool(i, {
                          rootVolumeSize: pool.rootVolumeSize + 1,
                        })
                      }
                      onChange={(e) => {
                        const val = Number(
                          (e.target as HTMLInputElement).value,
                        );
                        if (!isNaN(val) && val >= 1)
                          updatePool(i, { rootVolumeSize: val });
                      }}
                    />
                  </FormGroup>
                </GridItem>

                <GridItem span={6}>
                  <FormGroup
                    label="Root volume type"
                    isRequired
                    fieldId={`volume-type-${i}`}
                  >
                    <FormSelect
                      id={`volume-type-${i}`}
                      value={pool.rootVolumeType}
                      onChange={(_e, val) =>
                        updatePool(i, { rootVolumeType: val })
                      }
                    >
                      <FormSelectOption
                        value="pd-standard"
                        label="pd-standard"
                      />
                      <FormSelectOption value="pd-ssd" label="pd-ssd" />
                    </FormSelect>
                  </FormGroup>
                </GridItem>
                <GridItem span={6}>
                  <FormGroup
                    label="Upgrade type"
                    isRequired
                    fieldId={`upgrade-type-${i}`}
                  >
                    <FormSelect
                      id={`upgrade-type-${i}`}
                      value={pool.upgradeType}
                      onChange={(_e, val) =>
                        updatePool(i, { upgradeType: val })
                      }
                    >
                      <FormSelectOption value="Replace" label="Replace" />
                      <FormSelectOption value="InPlace" label="In-place" />
                    </FormSelect>
                  </FormGroup>
                </GridItem>

                <GridItem span={12}>
                  <Checkbox
                    id={`auto-repair-${i}`}
                    label="Enable auto-repair"
                    isChecked={pool.autoRepair}
                    onChange={(_e, checked) =>
                      updatePool(i, { autoRepair: checked })
                    }
                  />
                </GridItem>
              </Grid>
            </FormFieldGroupExpandable>
          ))}

          <Button variant="link" icon={<PlusCircleIcon />} onClick={addPool}>
            Add node pool
          </Button>
        </Form>
      )}

      {viewMode === "yaml" && (
        <>
          <CodeEditor
            language={Language.yaml}
            code={yamlText}
            onCodeChange={handleYamlChange}
            height="300px"
            isLineNumbersVisible
          />
          <HelperText>
            {!yamlValid && (
              <HelperTextItem variant="error">
                Invalid YAML syntax. Fix to sync changes.
              </HelperTextItem>
            )}
            {yamlValid && yamlErrors.length > 0 && (
              <>
                {yamlErrors.map((err, i) => (
                  <HelperTextItem key={i} variant="warning">
                    {err}
                  </HelperTextItem>
                ))}
              </>
            )}
            {yamlValid && yamlErrors.length === 0 && (
              <HelperTextItem variant="default">
                {formData.nodepools.length} node pool
                {formData.nodepools.length !== 1 ? "s" : ""} defined.
              </HelperTextItem>
            )}
          </HelperText>
        </>
      )}
    </>
  );
}
