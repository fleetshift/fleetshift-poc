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
import { PageHeader } from "@patternfly/react-component-groups/dist/dynamic/PageHeader";

import {
  type GcpHcpCluster,
  getGcpHcpCluster,
  deleteGcpHcpCluster,
  resumeGcpHcpCluster,
} from "./api";
import { stateLabel, formatTime } from "./gcpHcpUtils";
import GcpHcpDeliveryEventsTab from "./GcpHcpDeliveryEventsTab";

function OverviewTab({ cluster }: { cluster: GcpHcpCluster }) {
  const sl = stateLabel(cluster.state);
  const { spec } = cluster;

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
                <Label color={sl.color}>
                  {sl.text}
                  {cluster.reconciling ? " (reconciling)" : ""}
                </Label>
              </div>
            </CardBody>
          </Card>
        </GridItem>
        <GridItem span={3}>
          <Card isCompact isFullHeight>
            <CardBody>
              <Content component="p">Node Pools</Content>
              <Title headingLevel="h2" size="2xl">
                {spec.nodepools?.length ?? 0}
              </Title>
            </CardBody>
          </Card>
        </GridItem>
        <GridItem span={3}>
          <Card isCompact isFullHeight>
            <CardBody>
              <Content component="p">Version</Content>
              <Title headingLevel="h2" size="2xl">
                {spec.releaseVersion || "—"}
              </Title>
            </CardBody>
          </Card>
        </GridItem>
        <GridItem span={3}>
          <Card isCompact isFullHeight>
            <CardBody>
              <Content component="p">Endpoint Access</Content>
              <div style={{ marginTop: "var(--pf-t--global--spacer--sm)" }}>
                <Label color="blue" isCompact>
                  {spec.endpointAccess || "—"}
                </Label>
              </div>
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
                  <DescriptionListTerm>Cluster ID</DescriptionListTerm>
                  <DescriptionListDescription>
                    {cluster.name.replace(/^gCPHCPClusters\//, "")}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>UID</DescriptionListTerm>
                  <DescriptionListDescription>
                    {cluster.uid}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>Created</DescriptionListTerm>
                  <DescriptionListDescription>
                    {formatTime(cluster.createTime)}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>Channel Group</DescriptionListTerm>
                  <DescriptionListDescription>
                    {spec.channelGroup || "—"}
                  </DescriptionListDescription>
                </DescriptionListGroup>
              </DescriptionList>
            </GridItem>
            <GridItem span={6}>
              <DescriptionList>
                <DescriptionListGroup>
                  <DescriptionListTerm>Version</DescriptionListTerm>
                  <DescriptionListDescription>
                    {spec.releaseVersion || "—"}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>Updated</DescriptionListTerm>
                  <DescriptionListDescription>
                    {formatTime(cluster.updateTime)}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>Endpoint Access</DescriptionListTerm>
                  <DescriptionListDescription>
                    {spec.endpointAccess || "—"}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                {spec.nodepools?.length > 0 && (
                  <DescriptionListGroup>
                    <DescriptionListTerm>Node Pools</DescriptionListTerm>
                    <DescriptionListDescription>
                      {spec.nodepools
                        .map(
                          (np) =>
                            `${np.id} (${np.replicas}x ${np.instanceType})`,
                        )
                        .join(", ")}
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

export default function GcpHcpClusterDetailPage() {
  const { clusterId } = useParams<{ clusterId: string }>();
  const clusters = usePluginNavigate("gcphcp-plugin", "GcpHcpClustersModule");
  const [cluster, setCluster] = useState<GcpHcpCluster | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<string | number>("overview");
  const [actionsOpen, setActionsOpen] = useState(false);
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [isResuming, setIsResuming] = useState(false);

  const fetchCluster = useCallback(
    async (silent = false) => {
      if (!clusterId) return;
      if (!silent) setError(null);
      try {
        const data = await getGcpHcpCluster(clusterId);
        setCluster(data);
      } catch (e) {
        if (!silent)
          setError(e instanceof Error ? e.message : "Failed to load cluster");
      } finally {
        if (!silent) setLoading(false);
      }
    },
    [clusterId],
  );

  useEffect(() => {
    fetchCluster();
  }, [fetchCluster]);

  const isTransient =
    cluster?.state === "CREATING" ||
    cluster?.state === "DELETING" ||
    cluster?.reconciling;
  useEffect(() => {
    if (!isTransient) return;
    const id = setInterval(() => fetchCluster(true), 5000);
    return () => clearInterval(id);
  }, [isTransient, fetchCluster]);

  const clusterName = useMemo(() => clusterId ?? "", [clusterId]);

  const canResume =
    cluster?.state === "PAUSED_AUTH" || cluster?.state === "FAILED";
  const canDelete = cluster?.state !== "DELETING";

  const handleDelete = async () => {
    if (!clusterId) return;
    setIsDeleting(true);
    setShowDeleteModal(false);
    try {
      await deleteGcpHcpCluster(clusterId);
      clusters.navigate();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Delete failed");
      setIsDeleting(false);
    }
  };

  const handleResume = async () => {
    if (!clusterId) return;
    setIsResuming(true);
    setActionsOpen(false);
    try {
      await resumeGcpHcpCluster(clusterId);
      await fetchCluster();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Resume failed");
    } finally {
      setIsResuming(false);
    }
  };

  if (loading) {
    return <Spinner aria-label="Loading cluster details" />;
  }

  if (error || !cluster) {
    return (
      <EmptyState titleText={error ?? "Cluster not found"} headingLevel="h1">
        <EmptyStateBody>
          The requested GCP HCP cluster could not be loaded.
        </EmptyStateBody>
        <EmptyStateFooter>
          <PluginLink scope="gcphcp-plugin" module="GcpHcpClustersModule">
            Back to GCP HCP Clusters
          </PluginLink>
        </EmptyStateFooter>
      </EmptyState>
    );
  }

  const sl = stateLabel(cluster.state);

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
        subtitle={`Created ${formatTime(cluster.createTime)}`}
        label={
          <>
            <Label
              color={sl.color}
              isCompact
              style={{ marginRight: "var(--pf-t--global--spacer--sm)" }}
            >
              {sl.text}
              {cluster.reconciling ? " (reconciling)" : ""}
            </Label>
            <Label color="blue" isCompact>
              GCP HCP
            </Label>
          </>
        }
        breadcrumbs={
          <Breadcrumb>
            <BreadcrumbItem
              render={({ className, ariaCurrent }) => (
                <PluginLink
                  scope="gcphcp-plugin"
                  module="GcpHcpClustersModule"
                  className={className}
                  aria-current={ariaCurrent}
                >
                  GCP HCP Clusters
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
            <OverviewTab cluster={cluster} />
          </div>
        </Tab>
        <Tab eventKey="events" title={<TabTitleText>Events</TabTitleText>}>
          <div style={{ paddingTop: "var(--pf-t--global--spacer--md)" }}>
            <GcpHcpDeliveryEventsTab />
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
          description={`Are you sure you want to delete "${clusterName}"? This will terminate the provisioned GCP HCP cluster.`}
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
