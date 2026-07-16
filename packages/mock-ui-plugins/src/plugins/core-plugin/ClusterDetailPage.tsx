import "./ClusterDetailPage.scss";

import type { ClusterDetailTabProps } from "@fleetshift/common";
import { PluginLink, usePluginNavigate } from "@fleetshift/common";
import { useResolvedExtensions } from "@openshift/dynamic-plugin-sdk";
import { PageHeader } from "@patternfly/react-component-groups/dist/dynamic/PageHeader";
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
import type { ComponentType } from "react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useParams } from "react-router-dom";

import {
  deleteGcpHcpCluster,
  type GcpHcpCluster,
  getGcpHcpCluster,
  resumeGcpHcpCluster,
} from "../gcphcp-plugin/api";
import GcpHcpDeliveryEventsTab from "../gcphcp-plugin/GcpHcpDeliveryEventsTab";
import { formatTime, stateLabel } from "../gcphcp-plugin/gcpHcpUtils";

interface ResolvedTab {
  id: string;
  title: string;
  eventKey: string;
  priority: number;
  Component: ComponentType<ClusterDetailTabProps>;
}

const CLUSTER_DETAIL_TAB_TYPE = "fleetshift.cluster-detail-tab";

function isClusterDetailTabExtension(e: { type: string }): boolean {
  return e.type === CLUSTER_DETAIL_TAB_TYPE;
}

function OverviewTab({ cluster }: { cluster: GcpHcpCluster }) {
  const sl = stateLabel(cluster.state);
  const { spec } = cluster;

  return (
    <div className="ome-core-overview-layout">
      <Grid hasGutter>
        <GridItem span={3}>
          <Card isCompact isFullHeight>
            <CardBody>
              <Content component="p">Status</Content>
              <div className="pf-v6-u-mt-sm">
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
              <div className="pf-v6-u-mt-sm">
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
          <Title headingLevel="h2" size="lg" className="pf-v6-u-mb-md">
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

export default function ClusterDetailPage() {
  const { clusterId } = useParams<{ clusterId: string }>();
  const clusters = usePluginNavigate("core-plugin", "ClustersModule");
  const [cluster, setCluster] = useState<GcpHcpCluster | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<string | number>("overview");
  const [actionsOpen, setActionsOpen] = useState(false);
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [isResuming, setIsResuming] = useState(false);

  const [tabExtensions] = useResolvedExtensions(isClusterDetailTabExtension);
  const resolvedTabs = useMemo<ResolvedTab[]>(() => {
    const tabs: ResolvedTab[] = [];
    for (const ext of tabExtensions) {
      const p = ext.properties as unknown as {
        id: string;
        title: string;
        eventKey: string;
        priority?: number;
        component: ComponentType<ClusterDetailTabProps>;
      };
      tabs.push({
        id: p.id,
        title: p.title,
        eventKey: p.eventKey,
        priority: p.priority ?? 100,
        Component: p.component,
      });
    }
    return tabs.sort((a, b) => a.priority - b.priority);
  }, [tabExtensions]);

  const fetchCluster = useCallback(
    async (silent = false) => {
      if (!clusterId) return;
      if (!silent) setError(null);
      try {
        const data = await getGcpHcpCluster(clusterId);
        setCluster(data);
      } catch (e) {
        const message =
          e instanceof Error ? e.message : "Failed to load cluster";
        setError(message);
        if (silent) setCluster(null);
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

  const sl = stateLabel(cluster.state);

  return (
    <div className="ome-core-detail-layout">
      <PageHeader
        title={clusterName}
        subtitle={`Created ${formatTime(cluster.createTime)}`}
        label={
          <Label color={sl.color} isCompact className="pf-v6-u-mr-sm">
            {sl.text}
            {cluster.reconciling ? " (reconciling)" : ""}
          </Label>
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
          <div className="pf-v6-u-pt-md">
            <OverviewTab cluster={cluster} />
          </div>
        </Tab>
        <Tab eventKey="events" title={<TabTitleText>Events</TabTitleText>}>
          <div className="pf-v6-u-pt-md">
            <GcpHcpDeliveryEventsTab />
          </div>
        </Tab>
        {resolvedTabs.map((tab) => (
          <Tab
            key={tab.eventKey}
            eventKey={tab.eventKey}
            title={<TabTitleText>{tab.title}</TabTitleText>}
            mountOnEnter
            unmountOnExit
          >
            <div className="pf-v6-u-pt-md">
              <tab.Component clusterId={clusterId} />
            </div>
          </Tab>
        ))}
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
