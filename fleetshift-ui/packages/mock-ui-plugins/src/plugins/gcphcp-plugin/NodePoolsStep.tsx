import { CodeEditor, Language } from "@patternfly/react-code-editor";
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
                      {INSTANCE_TYPES.map((t) => (
                        <FormSelectOption key={t} value={t} label={t} />
                      ))}
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
                      {VOLUME_TYPES.map((t) => (
                        <FormSelectOption key={t} value={t} label={t} />
                      ))}
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
                      {UPGRADE_TYPES.map((t) => (
                        <FormSelectOption
                          key={t.value}
                          value={t.value}
                          label={t.label}
                        />
                      ))}
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
