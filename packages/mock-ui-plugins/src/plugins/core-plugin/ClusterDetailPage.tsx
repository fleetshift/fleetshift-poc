import { useState, useEffect, useCallback, useMemo } from "react";
import { useParams } from "react-router-dom";
import { PluginLink, usePluginNavigate } from "@fleetshift/common";
import {
  Breadcrumb,
  BreadcrumbItem,
  Button,
  Card,
  CardBody,
  Content,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  Dropdown,
  DropdownItem,
  DropdownList,
  EmptyState,
  EmptyStateBody,
  EmptyStateFooter,
  Grid,
  GridItem,
  Label,
  MenuToggle,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  Spinner,
  Tab,
  Tabs,
  TabTitleText,
  Title,
} from "@patternfly/react-core";
import { CubesIcon } from "@patternfly/react-icons";
import { PageHeader } from "@patternfly/react-component-groups/dist/dynamic/PageHeader";

import {
  getDeployment,
  deleteDeployment,
  resumeDeployment,
  type MgmtDeployment,
  type DeploymentState,
} from "../management-plugin/api";

interface KindClusterSpec {
  name: string;
  nodes?: Array<{ role: string; image?: string }>;
  networking?: {
    apiServerPort?: number;
    podSubnet?: string;
    serviceSubnet?: string;
  };
}

const STATE_LABELS: Record<
  DeploymentState,
  { text: string; color: "blue" | "green" | "red" | "orange" | "grey" }
> = {
  STATE_UNSPECIFIED: { text: "Unknown", color: "grey" },
  STATE_CREATING: { text: "Creating", color: "blue" },
  STATE_ACTIVE: { text: "Healthy", color: "green" },
  STATE_DELETING: { text: "Deleting", color: "orange" },
  STATE_FAILED: { text: "Failed", color: "red" },
  STATE_PAUSED_AUTH: { text: "Paused", color: "orange" },
};

function decodeSpec(deployment: MgmtDeployment): KindClusterSpec | null {
  const manifest = deployment.manifestStrategy?.manifests?.find(
    (m) => m.resourceType === "api.kind.cluster",
  );
  if (!manifest?.raw) return null;
  try {
    return JSON.parse(atob(manifest.raw));
  } catch {
    return null;
  }
}

function formatTime(iso: string): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

function OverviewTab({
  deployment,
  spec,
}: {
  deployment: MgmtDeployment;
  spec: KindClusterSpec | null;
}) {
  const nodeCount = spec?.nodes?.length ?? 0;
  const roles = useMemo(() => {
    if (!spec?.nodes) return "—";
    const counts: Record<string, number> = {};
    for (const n of spec.nodes) {
      counts[n.role] = (counts[n.role] ?? 0) + 1;
    }
    return Object.entries(counts)
      .map(([role, count]) => `${count} ${role}`)
      .join(", ");
  }, [spec]);

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: "var(--pf-t--global--spacer--lg)",
      }}
    >
      <Grid hasGutter>
        <GridItem span={3}>
          <Card isCompact isFullHeight>
            <CardBody>
              <Content component="p">Status</Content>
              <div style={{ marginTop: "var(--pf-t--global--spacer--sm)" }}>
                <Label
                  color={
                    (
                      STATE_LABELS[deployment.state] ??
                      STATE_LABELS.STATE_UNSPECIFIED
                    ).color
                  }
                >
                  {
                    (
                      STATE_LABELS[deployment.state] ??
                      STATE_LABELS.STATE_UNSPECIFIED
                    ).text
                  }
                  {deployment.reconciling ? " (reconciling)" : ""}
                </Label>
              </div>
            </CardBody>
          </Card>
        </GridItem>
        <GridItem span={3}>
          <Card isCompact isFullHeight>
            <CardBody>
              <Content component="p">Nodes</Content>
              <Title headingLevel="h2" size="2xl">
                {nodeCount}
              </Title>
            </CardBody>
          </Card>
        </GridItem>
        <GridItem span={3}>
          <Card isCompact isFullHeight>
            <CardBody>
              <Content component="p">Type</Content>
              <div style={{ marginTop: "var(--pf-t--global--spacer--sm)" }}>
                <Label color="blue">Kind</Label>
              </div>
            </CardBody>
          </Card>
        </GridItem>
        <GridItem span={3}>
          <Card isCompact isFullHeight>
            <CardBody>
              <Content component="p">Targets</Content>
              <Title headingLevel="h2" size="2xl">
                {deployment.resolvedTargetIds?.length ?? 0}
              </Title>
            </CardBody>
          </Card>
        </GridItem>
      </Grid>

      <Card>
        <CardBody>
          <Title
            headingLevel="h2"
            size="lg"
            style={{ marginBottom: "var(--pf-t--global--spacer--md)" }}
          >
            Cluster Information
          </Title>
          <Grid hasGutter>
            <GridItem span={6}>
              <DescriptionList>
                <DescriptionListGroup>
                  <DescriptionListTerm>Deployment ID</DescriptionListTerm>
                  <DescriptionListDescription>
                    {deployment.name.replace(/^deployments\//, "")}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>UID</DescriptionListTerm>
                  <DescriptionListDescription>
                    {deployment.uid}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>Created</DescriptionListTerm>
                  <DescriptionListDescription>
                    {formatTime(deployment.createTime)}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>Node Roles</DescriptionListTerm>
                  <DescriptionListDescription>
                    {roles}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>Placement</DescriptionListTerm>
                  <DescriptionListDescription>
                    {deployment.placementStrategy?.type?.replace("TYPE_", "") ??
                      "—"}
                  </DescriptionListDescription>
                </DescriptionListGroup>
              </DescriptionList>
            </GridItem>
            <GridItem span={6}>
              <DescriptionList>
                <DescriptionListGroup>
                  <DescriptionListTerm>Updated</DescriptionListTerm>
                  <DescriptionListDescription>
                    {formatTime(deployment.updateTime)}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                {spec?.networking?.apiServerPort && (
                  <DescriptionListGroup>
                    <DescriptionListTerm>API Server Port</DescriptionListTerm>
                    <DescriptionListDescription>
                      {spec.networking.apiServerPort}
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                )}
                {spec?.networking?.podSubnet && (
                  <DescriptionListGroup>
                    <DescriptionListTerm>Pod Subnet</DescriptionListTerm>
                    <DescriptionListDescription>
                      {spec.networking.podSubnet}
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                )}
                {spec?.networking?.serviceSubnet && (
                  <DescriptionListGroup>
                    <DescriptionListTerm>Service Subnet</DescriptionListTerm>
                    <DescriptionListDescription>
                      {spec.networking.serviceSubnet}
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                )}
                {deployment.resolvedTargetIds?.length > 0 && (
                  <DescriptionListGroup>
                    <DescriptionListTerm>Resolved Targets</DescriptionListTerm>
                    <DescriptionListDescription>
                      {deployment.resolvedTargetIds.join(", ")}
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                )}
              </DescriptionList>
            </GridItem>
          </Grid>
        </CardBody>
      </Card>
    </div>
  );
}

function LogsTab() {
  return (
    <EmptyState
      icon={CubesIcon}
      titleText="Logs not available"
      headingLevel="h2"
    >
      <EmptyStateBody>
        A logs and audit trail API is not yet available on the backend. This tab
        will show cluster provisioning and lifecycle events once the API is
        implemented.
      </EmptyStateBody>
      <EmptyStateFooter />
    </EmptyState>
  );
}

export default function ClusterDetailPage() {
  const { deploymentId } = useParams<{ deploymentId: string }>();
  const clusters = usePluginNavigate("core-plugin", "ClustersModule");
  const [deployment, setDeployment] = useState<MgmtDeployment | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<string | number>("overview");
  const [actionsOpen, setActionsOpen] = useState(false);
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [isResuming, setIsResuming] = useState(false);

  const fetchDeployment = useCallback(
    async (silent = false) => {
      if (!deploymentId) return;
      if (!silent) setError(null);
      try {
        const dep = await getDeployment(deploymentId);
        setDeployment(dep);
      } catch (e) {
        if (!silent)
          setError(e instanceof Error ? e.message : "Failed to load cluster");
      } finally {
        if (!silent) setLoading(false);
      }
    },
    [deploymentId],
  );

  useEffect(() => {
    fetchDeployment();
  }, [fetchDeployment]);

  const isTransient =
    deployment?.state === "STATE_CREATING" ||
    deployment?.state === "STATE_DELETING";
  useEffect(() => {
    if (!isTransient) return;
    const id = setInterval(() => fetchDeployment(true), 5000);
    return () => clearInterval(id);
  }, [isTransient, fetchDeployment]);

  const spec = useMemo(
    () => (deployment ? decodeSpec(deployment) : null),
    [deployment],
  );

  const clusterName = useMemo(() => {
    if (!deployment) return deploymentId ?? "";
    return spec?.name ?? deployment.name.replace(/^deployments\//, "");
  }, [deployment, spec, deploymentId]);

  const canResume =
    deployment?.state === "STATE_PAUSED_AUTH" ||
    deployment?.state === "STATE_FAILED";
  const canDelete = deployment?.state !== "STATE_DELETING";

  const handleDelete = async () => {
    if (!deploymentId) return;
    setIsDeleting(true);
    setShowDeleteModal(false);
    try {
      await deleteDeployment(deploymentId);
      clusters.navigate();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Delete failed");
      setIsDeleting(false);
    }
  };

  const handleResume = async () => {
    if (!deploymentId) return;
    setIsResuming(true);
    setActionsOpen(false);
    try {
      await resumeDeployment({ name: deploymentId });
      await fetchDeployment();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Resume failed");
    } finally {
      setIsResuming(false);
    }
  };

  if (loading) {
    return <Spinner aria-label="Loading cluster details" />;
  }

  if (error || !deployment) {
    return (
      <EmptyState titleText={error ?? "Cluster not found"} headingLevel="h1">
        <EmptyStateBody>
          The requested cluster could not be loaded.
        </EmptyStateBody>
        <EmptyStateFooter>
          <PluginLink scope="core-plugin" module="ClustersModule">
            Back to Clusters
          </PluginLink>
        </EmptyStateFooter>
      </EmptyState>
    );
  }

  const stateLabel =
    STATE_LABELS[deployment.state] ?? STATE_LABELS.STATE_UNSPECIFIED;

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: "var(--pf-t--global--spacer--md)",
      }}
    >
      <PageHeader
        title={clusterName}
        subtitle={`Created ${formatTime(deployment.createTime)}`}
        label={
          <>
            <Label
              color={stateLabel.color}
              isCompact
              style={{ marginRight: "var(--pf-t--global--spacer--sm)" }}
            >
              {stateLabel.text}
            </Label>
            <Label color="blue" isCompact>
              Kind
            </Label>
          </>
        }
        breadcrumbs={
          <Breadcrumb>
            <BreadcrumbItem
              render={({ className, ariaCurrent }) => (
                <PluginLink
                  scope="core-plugin"
                  module="ClustersModule"
                  className={className}
                  aria-current={ariaCurrent}
                >
                  Clusters
                </PluginLink>
              )}
            />
            <BreadcrumbItem isActive>{clusterName}</BreadcrumbItem>
          </Breadcrumb>
        }
        actionMenu={
          <Dropdown
            isOpen={actionsOpen}
            onOpenChange={setActionsOpen}
            toggle={(toggleRef) => (
              <MenuToggle
                ref={toggleRef}
                onClick={() => setActionsOpen((prev) => !prev)}
                variant="primary"
                isDisabled={isDeleting || isResuming}
              >
                Actions
              </MenuToggle>
            )}
            popperProps={{ position: "end" }}
          >
            <DropdownList>
              {canResume && (
                <DropdownItem
                  key="resume"
                  onClick={handleResume}
                  isDisabled={isResuming}
                >
                  {isResuming ? "Resuming..." : "Resume cluster"}
                </DropdownItem>
              )}
              {canDelete && (
                <DropdownItem
                  key="delete"
                  isDanger
                  onClick={() => {
                    setActionsOpen(false);
                    setShowDeleteModal(true);
                  }}
                >
                  Delete cluster
                </DropdownItem>
              )}
            </DropdownList>
          </Dropdown>
        }
      />

      <Tabs activeKey={activeTab} onSelect={(_e, key) => setActiveTab(key)}>
        <Tab eventKey="overview" title={<TabTitleText>Overview</TabTitleText>}>
          <div style={{ paddingTop: "var(--pf-t--global--spacer--md)" }}>
            <OverviewTab deployment={deployment} spec={spec} />
          </div>
        </Tab>
        <Tab eventKey="logs" title={<TabTitleText>Logs</TabTitleText>}>
          <div style={{ paddingTop: "var(--pf-t--global--spacer--md)" }}>
            <LogsTab />
          </div>
        </Tab>
      </Tabs>

      <Modal
        isOpen={showDeleteModal}
        onClose={() => setShowDeleteModal(false)}
        variant="small"
      >
        <ModalHeader
          title="Delete cluster"
          description={`Are you sure you want to delete "${clusterName}"? This will terminate the provisioned cluster.`}
        />
        <ModalBody />
        <ModalFooter>
          <Button
            variant="danger"
            onClick={handleDelete}
            isLoading={isDeleting}
            isDisabled={isDeleting}
          >
            Delete
          </Button>
          <Button
            variant="link"
            onClick={() => setShowDeleteModal(false)}
            isDisabled={isDeleting}
          >
            Cancel
          </Button>
        </ModalFooter>
      </Modal>
    </div>
  );
}
