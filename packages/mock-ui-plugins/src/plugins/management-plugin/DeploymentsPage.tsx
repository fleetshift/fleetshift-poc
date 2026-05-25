import { useState, useEffect, useCallback, useMemo, useRef } from "react";
import { Link } from "react-router-dom";
import {
  Alert,
  Bullseye,
  Button,
  DescriptionList,
  DescriptionListGroup,
  DescriptionListTerm,
  DescriptionListDescription,
  EmptyState,
  EmptyStateBody,
  Flex,
  FlexItem,
  Form,
  FormGroup,
  FormSelect,
  FormSelectOption,
  Label,
  LabelGroup,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  Pagination,
  SearchInput,
  Spinner,
  Switch,
  TextArea,
  TextInput,
  Title,
  Toolbar,
  ToolbarContent,
  ToolbarItem,
  Tooltip,
} from "@patternfly/react-core";
import {
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  ExpandableRowContent,
} from "@patternfly/react-table";
import {
  listDeployments,
  createDeployment,
  deleteDeployment,
  resumeDeployment,
} from "./api";
import { getModule } from "@scalprum/core";
import { buildSignedInputEnvelope } from "@fleetshift/common";
import type { MgmtDeployment, DeploymentState } from "./api";

const PER_PAGE = 20;

const STATE_LABELS: Record<
  DeploymentState,
  { text: string; color: "blue" | "green" | "red" | "orange" | "grey" }
> = {
  STATE_UNSPECIFIED: { text: "Unknown", color: "grey" },
  STATE_CREATING: { text: "Creating", color: "blue" },
  STATE_ACTIVE: { text: "Active", color: "green" },
  STATE_DELETING: { text: "Deleting", color: "orange" },
  STATE_FAILED: { text: "Failed", color: "red" },
  STATE_PAUSED_AUTH: { text: "Paused (Auth)", color: "orange" },
};

type PlacementType = "TYPE_STATIC" | "TYPE_ALL";

function formatTime(ts: string): string {
  if (!ts) return "—";
  try {
    return new Date(ts).toLocaleString();
  } catch {
    return ts;
  }
}

export default function DeploymentsPage() {
  const [deployments, setDeployments] = useState<MgmtDeployment[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [expandedName, setExpandedName] = useState<string | null>(null);
  const [nameFilter, setNameFilter] = useState("");
  const [page, setPage] = useState(1);

  const [hasSigningKey, setHasSigningKey] = useState(false);
  const signWithKeyRef = useRef<(bytes: Uint8Array) => Promise<string>>();
  useEffect(() => {
    Promise.all([
      getModule("signing-plugin", "signingKeyApi", "getSigningKeyStatus"),
      getModule("signing-plugin", "signingKeyApi", "signDeployment"),
    ]).then(
      ([getStatus, signFn]: [
        () => Promise<{ enrolled: boolean }>,
        (bytes: Uint8Array) => Promise<string>,
      ]) => {
        signWithKeyRef.current = signFn;
        getStatus().then((s) => setHasSigningKey(s.enrolled));
      },
    );
  }, []);

  // Create modal
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [modalError, setModalError] = useState<string | null>(null);
  const [signDeploymentEnabled, setSignDeploymentEnabled] = useState(false);

  // Form fields
  const [deploymentId, setDeploymentId] = useState("");
  const [resourceType, setResourceType] = useState("api.kind.cluster");
  const [manifestRaw, setManifestRaw] = useState("");
  const [placementType, setPlacementType] =
    useState<PlacementType>("TYPE_STATIC");
  const [targetIds, setTargetIds] = useState("kind-local");

  const fetchDeployments = useCallback(async (silent = false) => {
    if (!silent) {
      setLoading(true);
      setError(null);
    }
    try {
      const resp = await listDeployments();
      setDeployments(resp.deployments ?? []);
    } catch (err) {
      if (!silent) {
        setError(
          err instanceof Error ? err.message : "Failed to fetch deployments",
        );
      }
    } finally {
      if (!silent) setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchDeployments();
  }, [fetchDeployments]);

  // TODO: re-enable polling (or switch to gRPC streaming) once log noise is addressed
  // const hasTransient = deployments.some(
  //   (d) => d.state === "STATE_CREATING" || d.state === "STATE_DELETING",
  // );
  // useEffect(() => {
  //   if (!hasTransient) return;
  //   const id = setInterval(() => fetchDeployments(true), 5000);
  //   return () => clearInterval(id);
  // }, [hasTransient, fetchDeployments]);

  const filtered = useMemo(
    () =>
      deployments.filter((d) =>
        nameFilter
          ? d.name.toLowerCase().includes(nameFilter.toLowerCase())
          : true,
      ),
    [deployments, nameFilter],
  );

  const activeCount = useMemo(
    () => filtered.filter((d) => d.state === "STATE_ACTIVE").length,
    [filtered],
  );

  const paginatedItems = useMemo(
    () => filtered.slice((page - 1) * PER_PAGE, page * PER_PAGE),
    [filtered, page],
  );

  const resetForm = useCallback(() => {
    setDeploymentId("");
    setResourceType("api.kind.cluster");
    setManifestRaw("");
    setPlacementType("TYPE_STATIC");
    setTargetIds("kind-local");
    setSignDeploymentEnabled(false);
    setModalError(null);
  }, []);

  const handleOpen = useCallback(() => {
    resetForm();
    setIsModalOpen(true);
  }, [resetForm]);

  const handleClose = useCallback(() => {
    setIsModalOpen(false);
    resetForm();
  }, [resetForm]);

  const handleSubmit = useCallback(async () => {
    if (!deploymentId.trim()) {
      setModalError("Deployment ID is required.");
      return;
    }

    setCreating(true);
    setModalError(null);

    try {
      // For kind clusters, wrap in a ClusterSpec envelope.
      // When a custom config is provided, include it; otherwise send
      // just the name so the backend can auto-generate OIDC config
      // from the caller's token (config + authenticated caller is
      // rejected by the kind agent).
      let finalManifest: string;
      if (resourceType === "api.kind.cluster") {
        const spec: Record<string, unknown> = {
          name: deploymentId.trim(),
        };
        if (manifestRaw.trim()) {
          spec.config = JSON.parse(manifestRaw);
        }
        finalManifest = JSON.stringify(spec);
      } else {
        finalManifest = manifestRaw;
      }
      const rawBase64 = btoa(finalManifest);

      const parsedTargets =
        placementType === "TYPE_ALL"
          ? undefined
          : targetIds
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean);

      // Build signing fields if signing is enabled
      let userSignature: string | undefined;
      let validUntil: string | undefined;
      let expectedGeneration: number | undefined;

      if (signDeploymentEnabled) {
        const validUntilDate = new Date(Date.now() + 24 * 60 * 60 * 1000);
        validUntil = validUntilDate.toISOString();
        expectedGeneration = 1;

        // Canonical envelope uses domain-level type strings (matching Go),
        // not proto enum values (TYPE_INLINE → inline, TYPE_STATIC → static).
        const envelope = buildSignedInputEnvelope(
          deploymentId.trim(),
          {
            type: "inline",
            manifests: [
              {
                resourceType: resourceType,
                content: JSON.parse(finalManifest),
              },
            ],
          },
          placementType === "TYPE_ALL"
            ? { type: "all" }
            : { type: "static", targets: parsedTargets },
          validUntilDate,
          [],
          expectedGeneration,
        );

        // Web Crypto hashes internally — pass raw envelope bytes
        const envelopeBytes = new TextEncoder().encode(envelope);
        if (!signWithKeyRef.current)
          throw new Error("Signing module not loaded");
        userSignature = await signWithKeyRef.current(envelopeBytes);
      }

      await createDeployment({
        deploymentId: deploymentId.trim(),
        deployment: {
          manifestStrategy: {
            type: "TYPE_INLINE",
            manifests: [
              {
                resourceType: resourceType,
                raw: rawBase64,
              },
            ],
          },
          placementStrategy:
            placementType === "TYPE_ALL"
              ? { type: "TYPE_ALL" }
              : { type: "TYPE_STATIC", targetIds: parsedTargets },
          rolloutStrategy: { type: "TYPE_IMMEDIATE" },
        },
        userSignature,
        validUntil,
        expectedGeneration,
      });

      setSuccess(`Deployment "${deploymentId}" created.`);
      setIsModalOpen(false);
      resetForm();
      fetchDeployments();
    } catch (err) {
      setModalError(
        err instanceof Error ? err.message : "Failed to create deployment",
      );
    } finally {
      setCreating(false);
    }
  }, [
    deploymentId,
    resourceType,
    manifestRaw,
    placementType,
    targetIds,
    signDeploymentEnabled,
    resetForm,
    fetchDeployments,
  ]);

  const handleDelete = useCallback(
    async (name: string) => {
      try {
        await deleteDeployment(name.replace("deployments/", ""));
        setSuccess(`Deployment "${name}" deletion initiated.`);
        fetchDeployments();
      } catch (err) {
        setError(
          err instanceof Error ? err.message : "Failed to delete deployment",
        );
      }
    },
    [fetchDeployments],
  );

  const handleResume = useCallback(
    async (name: string) => {
      try {
        await resumeDeployment({
          name: name.replace("deployments/", ""),
        });
        setSuccess(`Deployment "${name}" resumed.`);
        fetchDeployments();
      } catch (err) {
        setError(
          err instanceof Error ? err.message : "Failed to resume deployment",
        );
      }
    },
    [fetchDeployments],
  );

  const toggleExpand = useCallback((name: string) => {
    setExpandedName((prev) => (prev === name ? null : name));
  }, []);

  if (loading) {
    return (
      <Bullseye>
        <Spinner />
      </Bullseye>
    );
  }

  return (
    <div>
      <Flex
        alignItems={{ default: "alignItemsBaseline" }}
        gap={{ default: "gapSm" }}
        style={{ marginBottom: "var(--pf-t--global--spacer--lg)" }}
      >
        <FlexItem>
          <Title headingLevel="h1">Orchestration</Title>
        </FlexItem>
        <FlexItem>
          <span
            style={{
              fontSize: "var(--pf-t--global--font--size--sm)",
              color: "var(--pf-t--global--text--color--subtle)",
            }}
          >
            {activeCount} active
            {deployments.length !== activeCount &&
              ` / ${deployments.length} total`}
          </span>
        </FlexItem>
      </Flex>

      {success && (
        <Alert
          variant="success"
          title={success}
          isInline
          actionClose={
            <Button variant="plain" onClick={() => setSuccess(null)}>
              &times;
            </Button>
          }
          style={{ marginBottom: "var(--pf-t--global--spacer--md)" }}
        />
      )}
      {error && (
        <Alert
          variant="danger"
          title={error}
          isInline
          actionClose={
            <Button variant="plain" onClick={() => setError(null)}>
              &times;
            </Button>
          }
          style={{ marginBottom: "var(--pf-t--global--spacer--md)" }}
        />
      )}

      {deployments.length === 0 ? (
        <EmptyState titleText="No deployments" headingLevel="h2">
          <EmptyStateBody>
            Create an orchestration deployment to provision kind clusters or
            deploy manifests to registered targets.
          </EmptyStateBody>
          <Button variant="primary" onClick={handleOpen}>
            Create deployment
          </Button>
        </EmptyState>
      ) : (
        <>
          <Toolbar
            clearAllFilters={() => {
              setNameFilter("");
              setPage(1);
            }}
          >
            <ToolbarContent>
              <ToolbarItem>
                <SearchInput
                  placeholder="Filter by name"
                  value={nameFilter}
                  onChange={(_event, value) => {
                    setNameFilter(value);
                    setPage(1);
                  }}
                  onClear={() => {
                    setNameFilter("");
                    setPage(1);
                  }}
                />
              </ToolbarItem>
              <ToolbarItem>
                <Button variant="primary" onClick={handleOpen}>
                  Create deployment
                </Button>
              </ToolbarItem>
              <ToolbarItem variant="pagination" align={{ default: "alignEnd" }}>
                <Pagination
                  itemCount={filtered.length}
                  perPage={PER_PAGE}
                  page={page}
                  onSetPage={(_event, p) => setPage(p)}
                  isCompact
                />
              </ToolbarItem>
            </ToolbarContent>
          </Toolbar>

          {filtered.length === 0 ? (
            <EmptyState titleText="No matching deployments" headingLevel="h2">
              <EmptyStateBody>
                No deployments match the current filter. Try adjusting or
                clearing the filter.
              </EmptyStateBody>
              <Button
                variant="link"
                onClick={() => {
                  setNameFilter("");
                  setPage(1);
                }}
              >
                Clear filter
              </Button>
            </EmptyState>
          ) : (
            <>
              <Table
                aria-label="Orchestration deployments"
                variant="compact"
                hasAnimations
              >
                <Thead>
                  <Tr>
                    <Th screenReaderText="Row expansion" />
                    <Th>Name</Th>
                    <Th>State</Th>
                    <Th>Targets</Th>
                    <Th>Created</Th>
                    <Th screenReaderText="Actions" />
                  </Tr>
                </Thead>
                {paginatedItems.map((d, rowIndex) => {
                  const stateInfo =
                    STATE_LABELS[d.state] ?? STATE_LABELS.STATE_UNSPECIFIED;
                  return (
                    <Tbody key={d.name} isExpanded={expandedName === d.name}>
                      <Tr>
                        <Td
                          expand={{
                            rowIndex,
                            isExpanded: expandedName === d.name,
                            onToggle: () => toggleExpand(d.name),
                          }}
                        />
                        <Td dataLabel="Name">
                          <Link
                            to={`/orchestration/${d.name.replace("deployments/", "")}`}
                            style={{
                              fontWeight:
                                "var(--pf-t--global--font--weight--heading--default)",
                            }}
                          >
                            {d.name.replace("deployments/", "")}
                          </Link>
                        </Td>
                        <Td dataLabel="State">
                          <Label color={stateInfo.color} isCompact>
                            {stateInfo.text}
                            {d.reconciling ? " (reconciling)" : ""}
                          </Label>
                        </Td>
                        <Td dataLabel="Targets">
                          {(d.resolvedTargetIds ?? []).length > 0 ? (
                            <LabelGroup>
                              {d.resolvedTargetIds.map((t) => (
                                <Label key={t} color="blue" isCompact>
                                  {t}
                                </Label>
                              ))}
                            </LabelGroup>
                          ) : (
                            <span
                              style={{
                                color:
                                  "var(--pf-t--global--text--color--subtle)",
                              }}
                            >
                              —
                            </span>
                          )}
                        </Td>
                        <Td dataLabel="Created">
                          <span
                            style={{
                              fontSize: "var(--pf-t--global--font--size--sm)",
                              color: "var(--pf-t--global--text--color--subtle)",
                            }}
                          >
                            {formatTime(d.createTime)}
                          </span>
                        </Td>
                        <Td dataLabel="Actions" isActionCell>
                          <Flex gap={{ default: "gapSm" }}>
                            {d.state === "STATE_PAUSED_AUTH" && (
                              <Button
                                variant="secondary"
                                size="sm"
                                onClick={() => handleResume(d.name)}
                              >
                                Resume
                              </Button>
                            )}
                            {d.state !== "STATE_DELETING" &&
                              !d.name.includes("idp-trust-") && (
                                <Button
                                  variant="danger"
                                  size="sm"
                                  onClick={() => handleDelete(d.name)}
                                >
                                  Delete
                                </Button>
                              )}
                          </Flex>
                        </Td>
                      </Tr>
                      <Tr isExpanded={expandedName === d.name}>
                        <Td />
                        <Td colSpan={5}>
                          <ExpandableRowContent>
                            <DescriptionList isHorizontal isCompact>
                              <DescriptionListGroup>
                                <DescriptionListTerm>UID</DescriptionListTerm>
                                <DescriptionListDescription>
                                  <span
                                    style={{
                                      fontFamily:
                                        "var(--pf-t--global--font--family--mono)",
                                      fontSize:
                                        "var(--pf-t--global--font--size--sm)",
                                    }}
                                  >
                                    {d.uid}
                                  </span>
                                </DescriptionListDescription>
                              </DescriptionListGroup>
                              <DescriptionListGroup>
                                <DescriptionListTerm>
                                  Manifest Strategy
                                </DescriptionListTerm>
                                <DescriptionListDescription>
                                  <Flex
                                    gap={{ default: "gapSm" }}
                                    alignItems={{ default: "alignItemsCenter" }}
                                  >
                                    <span>
                                      {d.manifestStrategy?.type ?? "—"}
                                    </span>
                                    {d.manifestStrategy?.manifests?.map(
                                      (m, i) => (
                                        <Label key={i} color="grey" isCompact>
                                          {m.resourceType}
                                        </Label>
                                      ),
                                    )}
                                  </Flex>
                                </DescriptionListDescription>
                              </DescriptionListGroup>
                              <DescriptionListGroup>
                                <DescriptionListTerm>
                                  Placement Strategy
                                </DescriptionListTerm>
                                <DescriptionListDescription>
                                  <Flex
                                    gap={{ default: "gapSm" }}
                                    alignItems={{ default: "alignItemsCenter" }}
                                  >
                                    <span>
                                      {d.placementStrategy?.type ?? "—"}
                                    </span>
                                    {d.placementStrategy?.targetIds && (
                                      <LabelGroup>
                                        {d.placementStrategy.targetIds.map(
                                          (t) => (
                                            <Label key={t} isCompact>
                                              {t}
                                            </Label>
                                          ),
                                        )}
                                      </LabelGroup>
                                    )}
                                  </Flex>
                                </DescriptionListDescription>
                              </DescriptionListGroup>
                              <DescriptionListGroup>
                                <DescriptionListTerm>
                                  Rollout Strategy
                                </DescriptionListTerm>
                                <DescriptionListDescription>
                                  {d.rolloutStrategy?.type ?? "Immediate"}
                                </DescriptionListDescription>
                              </DescriptionListGroup>
                              <DescriptionListGroup>
                                <DescriptionListTerm>
                                  Updated
                                </DescriptionListTerm>
                                <DescriptionListDescription>
                                  {formatTime(d.updateTime)}
                                </DescriptionListDescription>
                              </DescriptionListGroup>
                              <DescriptionListGroup>
                                <DescriptionListTerm>ETag</DescriptionListTerm>
                                <DescriptionListDescription>
                                  <span
                                    style={{
                                      fontFamily:
                                        "var(--pf-t--global--font--family--mono)",
                                      fontSize:
                                        "var(--pf-t--global--font--size--sm)",
                                    }}
                                  >
                                    {d.etag}
                                  </span>
                                </DescriptionListDescription>
                              </DescriptionListGroup>
                            </DescriptionList>
                          </ExpandableRowContent>
                        </Td>
                      </Tr>
                    </Tbody>
                  );
                })}
              </Table>
              <Toolbar>
                <ToolbarContent>
                  <ToolbarItem
                    variant="pagination"
                    align={{ default: "alignEnd" }}
                  >
                    <Pagination
                      itemCount={filtered.length}
                      perPage={PER_PAGE}
                      page={page}
                      onSetPage={(_event, p) => setPage(p)}
                      isCompact
                    />
                  </ToolbarItem>
                </ToolbarContent>
              </Toolbar>
            </>
          )}
        </>
      )}

      <Modal
        variant="large"
        isOpen={isModalOpen}
        onClose={handleClose}
        aria-labelledby="create-deployment-modal-title"
      >
        <ModalHeader
          title="Create Orchestration Deployment"
          labelId="create-deployment-modal-title"
        />
        <ModalBody>
          <Form>
            {modalError && (
              <Alert
                variant="danger"
                title={modalError}
                isInline
                style={{ marginBottom: "var(--pf-t--global--spacer--md)" }}
              />
            )}

            <FormGroup label="Cluster Name" isRequired fieldId="deployment-id">
              <TextInput
                id="deployment-id"
                isRequired
                value={deploymentId}
                onChange={(_e, v) => setDeploymentId(v)}
                placeholder="dev-cluster"
              />
            </FormGroup>

            <FormGroup label="Resource Type" fieldId="resource-type">
              <FormSelect
                id="resource-type"
                value={resourceType}
                onChange={(_e, v) => setResourceType(v)}
              >
                <FormSelectOption
                  value="api.kind.cluster"
                  label="Kind Cluster"
                />
              </FormSelect>
            </FormGroup>

            <FormGroup label="Kind Config (optional)" fieldId="manifest-raw">
              <TextArea
                id="manifest-raw"
                value={manifestRaw}
                onChange={(_e, v) => setManifestRaw(v)}
                rows={6}
                resizeOrientation="vertical"
                placeholder="Leave empty to use default config. When logged in via OIDC, the cluster is auto-configured with your identity."
                style={{
                  fontFamily: "var(--pf-t--global--font--family--mono)",
                }}
              />
            </FormGroup>

            <FormGroup label="Placement" fieldId="placement-type">
              <FormSelect
                id="placement-type"
                value={placementType}
                onChange={(_e, v) => setPlacementType(v as PlacementType)}
              >
                <FormSelectOption
                  value="TYPE_STATIC"
                  label="Static (specific targets)"
                />
                <FormSelectOption value="TYPE_ALL" label="All targets" />
              </FormSelect>
            </FormGroup>

            {placementType === "TYPE_STATIC" && (
              <FormGroup
                label="Target IDs (comma-separated)"
                fieldId="target-ids"
              >
                <TextInput
                  id="target-ids"
                  value={targetIds}
                  onChange={(_e, v) => setTargetIds(v)}
                  placeholder="kind-local"
                />
              </FormGroup>
            )}

            <FormGroup fieldId="sign-deployment">
              {hasSigningKey ? (
                <Switch
                  id="sign-deployment"
                  label="Sign deployment"
                  isChecked={signDeploymentEnabled}
                  onChange={(_e, checked) => setSignDeploymentEnabled(checked)}
                />
              ) : (
                <Tooltip content="Enroll a signing key first (Signing Keys page)">
                  <Switch
                    id="sign-deployment"
                    label="Sign deployment"
                    isChecked={false}
                    isDisabled
                  />
                </Tooltip>
              )}
            </FormGroup>
          </Form>
        </ModalBody>
        <ModalFooter>
          <Button
            variant="primary"
            onClick={handleSubmit}
            isDisabled={creating}
            isLoading={creating}
          >
            {creating
              ? signDeploymentEnabled
                ? "Signing & Creating..."
                : "Creating..."
              : signDeploymentEnabled
                ? "Sign & Create"
                : "Create"}
          </Button>
          <Button variant="link" onClick={handleClose} isDisabled={creating}>
            Cancel
          </Button>
        </ModalFooter>
      </Modal>
    </div>
  );
}
