import { CodeEditor, Language } from "@patternfly/react-code-editor";
import {
  Button,
  Checkbox,
  Flex,
  FlexItem,
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
import PlusCircleIcon from "@patternfly/react-icons/dist/dynamic/icons/plus-circle-icon";
import TrashIcon from "@patternfly/react-icons/dist/dynamic/icons/trash-icon";
import { useCallback, useEffect, useState } from "react";

import type { GcpHcpFormData, NodepoolEntry } from "./CreateGcpHcpWizard";
import {
  DEFAULT_NODEPOOL,
  INSTANCE_TYPES,
  parseFromYaml,
  serializeToYaml,
  UPGRADE_TYPES,
  validatePools,
  VOLUME_TYPES,
} from "./nodePoolYaml";

interface SelectOption {
  value: string;
  label: string;
}

type PoolFieldConfig =
  | {
      type: "text";
      key: string & keyof NodepoolEntry;
      label: string;
      span?: number;
      placeholder?: string;
      validate?: (value: string) => "default" | "error";
    }
  | {
      type: "number";
      key: string & keyof NodepoolEntry;
      label: string;
      span?: number;
      min: number;
    }
  | {
      type: "select";
      key: string & keyof NodepoolEntry;
      label: string;
      span?: number;
      options: SelectOption[];
    }
  | {
      type: "checkbox";
      key: string & keyof NodepoolEntry;
      label: string;
      span?: number;
    };

const POOL_ID_PATTERN = /^[a-z][-a-z0-9]*$/;

const POOL_FIELDS: PoolFieldConfig[] = [
  {
    type: "text",
    key: "id",
    label: "Pool ID",
    placeholder: "workers",
    validate: (v) =>
      !v.trim() ? "default" : POOL_ID_PATTERN.test(v) ? "default" : "error",
  },
  { type: "number", key: "replicas", label: "Replicas", min: 1 },
  {
    type: "select",
    key: "instanceType",
    label: "Instance type",
    options: INSTANCE_TYPES.map((t) => ({ value: t, label: t })),
  },
  {
    type: "number",
    key: "rootVolumeSize",
    label: "Root volume size (GB)",
    min: 1,
  },
  {
    type: "select",
    key: "rootVolumeType",
    label: "Root volume type",
    options: VOLUME_TYPES.map((t) => ({ value: t, label: t })),
  },
  {
    type: "select",
    key: "upgradeType",
    label: "Upgrade type",
    options: UPGRADE_TYPES,
  },
  {
    type: "checkbox",
    key: "autoRepair",
    label: "Enable auto-repair",
    span: 12,
  },
];

interface PoolFieldProps {
  config: PoolFieldConfig;
  pool: NodepoolEntry;
  index: number;
  onUpdate: (index: number, patch: Partial<NodepoolEntry>) => void;
}

function PoolField({ config, pool, index, onUpdate }: PoolFieldProps) {
  const fieldId = `${config.key}-${index}`;
  const update = (value: NodepoolEntry[string & keyof NodepoolEntry]) =>
    onUpdate(index, { [config.key]: value } as Partial<NodepoolEntry>);

  switch (config.type) {
    case "checkbox":
      return (
        <Checkbox
          id={fieldId}
          label={config.label}
          isChecked={pool[config.key] as boolean}
          onChange={(_e, checked) => update(checked)}
        />
      );

    case "text":
      return (
        <FormGroup label={config.label} isRequired fieldId={fieldId}>
          <TextInput
            id={fieldId}
            isRequired
            value={pool[config.key] as string}
            onChange={(_e, val) => update(val)}
            placeholder={config.placeholder}
            validated={
              config.validate?.(pool[config.key] as string) ?? "default"
            }
          />
        </FormGroup>
      );

    case "number": {
      const numValue = pool[config.key] as number;
      return (
        <FormGroup label={config.label} isRequired fieldId={fieldId}>
          <NumberInput
            id={fieldId}
            value={numValue}
            min={config.min}
            onMinus={() => update(Math.max(config.min, numValue - 1))}
            onPlus={() => update(numValue + 1)}
            onChange={(e) => {
              const val = Number((e.target as HTMLInputElement).value);
              if (!isNaN(val) && val >= config.min) update(val);
            }}
          />
        </FormGroup>
      );
    }

    case "select":
      return (
        <FormGroup label={config.label} isRequired fieldId={fieldId}>
          <FormSelect
            id={fieldId}
            value={pool[config.key] as string}
            onChange={(_e, val) => update(val)}
          >
            {config.options.map((o) => (
              <FormSelectOption key={o.value} value={o.value} label={o.label} />
            ))}
          </FormSelect>
        </FormGroup>
      );
  }
}

interface NodePoolsStepProps {
  formData: GcpHcpFormData;
  onChange: <K extends keyof GcpHcpFormData>(
    field: K,
    value: GcpHcpFormData[K],
  ) => void;
}

function poolSummary(pool: NodepoolEntry): string {
  return `${pool.replicas}x ${pool.instanceType}, ${pool.rootVolumeSize}GB ${pool.rootVolumeType}`;
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
        <div className="pf-v6-c-form">
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
                {POOL_FIELDS.map((field) => (
                  <GridItem key={field.key} span={field.span ?? 6}>
                    <PoolField
                      config={field}
                      pool={pool}
                      index={i}
                      onUpdate={updatePool}
                    />
                  </GridItem>
                ))}
              </Grid>
            </FormFieldGroupExpandable>
          ))}

          <Button variant="link" icon={<PlusCircleIcon />} onClick={addPool}>
            Add node pool
          </Button>
        </div>
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
