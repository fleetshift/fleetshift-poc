import { useState, useEffect, useCallback, useMemo } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  Breadcrumb,
  BreadcrumbItem,
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
  Spinner,
  Tab,
  Tabs,
  TabTitleText,
  Title,
} from "@patternfly/react-core";
import { CubesIcon } from "@patternfly/react-icons";
import { PageHeader } from "@patternfly/react-component-groups/dist/dynamic/PageHeader";
import { Link } from "react-router-dom";

import {
  getDeployment,
  deleteDeployment,
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
  const navigate = useNavigate();
  const [deployment, setDeployment] = useState<MgmtDeployment | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<string | number>("overview");
  const [actionsOpen, setActionsOpen] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);

  const fetchDeployment = useCallback(async () => {
    if (!deploymentId) return;
    try {
      const dep = await getDeployment(deploymentId);
      setDeployment(dep);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load cluster");
    } finally {
      setLoading(false);
    }
  }, [deploymentId]);

  useEffect(() => {
    fetchDeployment();
  }, [fetchDeployment]);

  const spec = useMemo(
    () => (deployment ? decodeSpec(deployment) : null),
    [deployment],
  );

  const clusterName = useMemo(() => {
    if (!deployment) return deploymentId ?? "";
    return spec?.name ?? deployment.name.replace(/^deployments\//, "");
  }, [deployment, spec, deploymentId]);

  const handleDelete = async () => {
    if (!deploymentId) return;
    setIsDeleting(true);
    setActionsOpen(false);
    try {
      await deleteDeployment(deploymentId);
      navigate("..");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Delete failed");
      setIsDeleting(false);
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
          <Link to="..">Back to Clusters</Link>
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
            <BreadcrumbItem>
              <Link to="..">Clusters</Link>
            </BreadcrumbItem>
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
                isDisabled={isDeleting}
              >
                Actions
              </MenuToggle>
            )}
            popperProps={{ position: "end" }}
          >
            <DropdownList>
              <DropdownItem key="delete" isDanger onClick={handleDelete}>
                Delete cluster
              </DropdownItem>
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
    </div>
  );
}
