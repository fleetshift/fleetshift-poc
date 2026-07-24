import "./ClusterDetailPage.scss";

import type { ClusterDetailTabProps, ResourceResult } from "@fleetshift/common";
import {
  createApiClient,
  createResourceApi,
  PluginLink,
  usePluginNavigate,
} from "@fleetshift/common";
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
import { PauseCircleIcon } from "@patternfly/react-icons";
import type { ComponentType } from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";

import {
  buildAddonBasePath,
  type ClusterResource,
  deriveClusterState,
  extractClusterId,
  extractService,
  formatTime,
  isTransientState,
  stateLabel,
} from "./clusterTypes";

const clusterApi = createResourceApi<ClusterResource>("-");

const CLUSTER_TYPE_FILTER =
  'resourceType == "gcphcp.fleetshift.io/Cluster" || resourceType == "kind.fleetshift.io/Cluster"';

interface ResolvedTab {
  id: string;
  title: string;
  eventKey: string;
  priority: number;
  service?: string;
  Component: ComponentType<ClusterDetailTabProps>;
}

const CLUSTER_DETAIL_TAB_TYPE = "fleetshift.cluster-detail-tab";

function isClusterDetailTabExtension(e: { type: string }): boolean {
  return e.type === CLUSTER_DETAIL_TAB_TYPE;
}

function OverviewTab({
  result,
  service,
}: {
  result: ResourceResult<ClusterResource>;
  service: string;
}) {
  const cluster = result.resource;
  const state = deriveClusterState(cluster);
  const sl = stateLabel(state, cluster.state);
  const { spec } = cluster;
  const isGcpHcp = service === "gcphcp.fleetshift.io";
  const isKind = service === "kind.fleetshift.io";

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
              <Content component="p">
                {isGcpHcp ? "Node Pools" : "Provider"}
              </Content>
              <Title headingLevel="h2" size="2xl">
                {isGcpHcp
                  ? (spec?.nodepools?.length ?? 0)
                  : isKind
                    ? "Kind"
                    : "—"}
              </Title>
            </CardBody>
          </Card>
        </GridItem>
        <GridItem span={3}>
          <Card isCompact isFullHeight>
            <CardBody>
              <Content component="p">Version</Content>
              <Title headingLevel="h2" size="2xl">
                {spec?.releaseVersion || "—"}
              </Title>
            </CardBody>
          </Card>
        </GridItem>
        <GridItem span={3}>
          <Card isCompact isFullHeight>
            <CardBody>
              <Content component="p">
                {isGcpHcp ? "Endpoint Access" : "API Server"}
              </Content>
              <div className="pf-v6-u-mt-sm">
                {isGcpHcp ? (
                  <Label color="blue" isCompact>
                    {spec?.endpointAccess || "—"}
                  </Label>
                ) : isKind ? (
                  <Label
                    color={
                      cluster.conditions?.APIServerAvailable?.status === "True"
                        ? "green"
                        : "grey"
                    }
                    isCompact
                  >
                    {cluster.conditions?.APIServerAvailable?.status === "True"
                      ? "Available"
                      : "Unavailable"}
                  </Label>
                ) : (
                  <Label color="grey" isCompact>
                    —
                  </Label>
                )}
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
                    {extractClusterId(cluster.name)}
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
                {isGcpHcp && (
                  <DescriptionListGroup>
                    <DescriptionListTerm>Channel Group</DescriptionListTerm>
                    <DescriptionListDescription>
                      {spec?.channelGroup || "—"}
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                )}
              </DescriptionList>
            </GridItem>
            <GridItem span={6}>
              <DescriptionList>
                <DescriptionListGroup>
                  <DescriptionListTerm>Version</DescriptionListTerm>
                  <DescriptionListDescription>
                    {spec?.releaseVersion || "—"}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>Updated</DescriptionListTerm>
                  <DescriptionListDescription>
                    {formatTime(cluster.updateTime)}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                {isGcpHcp && spec?.endpointAccess && (
                  <DescriptionListGroup>
                    <DescriptionListTerm>Endpoint Access</DescriptionListTerm>
                    <DescriptionListDescription>
                      {spec.endpointAccess}
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                )}
                {isGcpHcp && spec?.nodepools && spec.nodepools.length > 0 && (
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

      {isKind && cluster.conditions && (
        <Card>
          <CardBody>
            <Title headingLevel="h2" size="lg" className="pf-v6-u-mb-md">
              Conditions
            </Title>
            <DescriptionList isHorizontal>
              {Object.entries(cluster.conditions).map(([type, cond]) => (
                <DescriptionListGroup key={type}>
                  <DescriptionListTerm>{type}</DescriptionListTerm>
                  <DescriptionListDescription>
                    <Label
                      isCompact
                      color={cond.status === "True" ? "green" : "grey"}
                    >
                      {cond.status}
                    </Label>
                    {cond.reason && (
                      <span className="pf-v6-u-ml-sm">{cond.reason}</span>
                    )}
                  </DescriptionListDescription>
                </DescriptionListGroup>
              ))}
            </DescriptionList>
          </CardBody>
        </Card>
      )}

      {cluster.pauseReason && (
        <Card>
          <CardBody>
            <Title headingLevel="h2" size="lg" className="pf-v6-u-mb-md">
              Pause Reason
            </Title>
            <Content component="p">{cluster.pauseReason}</Content>
          </CardBody>
        </Card>
      )}
    </div>
  );
}

export default function ClusterDetailPage() {
  const { clusterId } = useParams<{ clusterId: string }>();
  const clusters = usePluginNavigate("core-plugin", "ClustersModule");
  const [result, setResult] = useState<ResourceResult<ClusterResource> | null>(
    null,
  );
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchParams, setSearchParams] = useSearchParams();
  const activeTab = searchParams.get("tab") ?? "overview";
  const setActiveTab = useCallback(
    (key: string | number) => {
      const tab = String(key);
      setSearchParams(
        (prev) => {
          if (tab === "overview") {
            prev.delete("tab");
          } else {
            prev.set("tab", tab);
          }
          return prev;
        },
        { replace: true },
      );
    },
    [setSearchParams],
  );
  const [actionsOpen, setActionsOpen] = useState(false);
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [isResuming, setIsResuming] = useState(false);
  const requestIdRef = useRef(0);

  const [tabExtensions] = useResolvedExtensions(isClusterDetailTabExtension);
  const resolvedTabs = useMemo<ResolvedTab[]>(() => {
    const tabs: ResolvedTab[] = [];
    for (const ext of tabExtensions) {
      const p = ext.properties as unknown as {
        id: string;
        title: string;
        eventKey: string;
        priority?: number;
        service?: string;
        component: ComponentType<ClusterDetailTabProps>;
      };
      tabs.push({
        id: p.id,
        title: p.title,
        eventKey: p.eventKey,
        priority: p.priority ?? 100,
        service: p.service,
        Component: p.component,
      });
    }
    return tabs.sort((a, b) => a.priority - b.priority);
  }, [tabExtensions]);

  const service = result ? extractService(result.name) : "";

  const fetchCluster = useCallback(
    async (silent = false) => {
      if (!clusterId) return;
      const id = ++requestIdRef.current;
      if (!silent) setError(null);
      try {
        const escaped = clusterId.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
        const response = await clusterApi.search({
          filter: `(${CLUSTER_TYPE_FILTER}) && resource.name == "clusters/${escaped}"`,
          pageSize: 1,
        });
        if (id !== requestIdRef.current) return;
        if (response.resources.length > 0) {
          setResult(response.resources[0]);
        } else {
          if (silent) setResult(null);
          else setError("Cluster not found");
        }
      } catch (e) {
        if (id !== requestIdRef.current) return;
        if (!silent) {
          const message =
            e instanceof Error ? e.message : "Failed to load cluster";
          setError(message);
        }
      } finally {
        if (id === requestIdRef.current && !silent) setLoading(false);
      }
    },
    [clusterId],
  );

  useEffect(() => {
    fetchCluster();
  }, [fetchCluster]);

  const cluster = result?.resource;
  const state = cluster ? deriveClusterState(cluster) : undefined;
  const isTransient =
    isTransientState(state) || (cluster?.reconciling ?? false);

  useEffect(() => {
    if (!isTransient) return;
    const id = setInterval(() => fetchCluster(true), 5000);
    return () => clearInterval(id);
  }, [isTransient, fetchCluster]);

  const clusterName = useMemo(() => clusterId ?? "", [clusterId]);

  const isGcpHcp = service === "gcphcp.fleetshift.io";
  const canResume = isGcpHcp && (state === "PAUSED_AUTH" || state === "FAILED");
  const canDelete = state !== "DELETING";

  const handleDelete = async () => {
    if (!clusterId || !service) return;
    setIsDeleting(true);
    setShowDeleteModal(false);
    try {
      const client = createApiClient(buildAddonBasePath(service));
      await client.delete(`/clusters/${clusterId}`);
      clusters.navigate();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Delete failed");
      setIsDeleting(false);
    }
  };

  const handleResume = async () => {
    if (!clusterId || !service) return;
    setIsResuming(true);
    setActionsOpen(false);
    try {
      const client = createApiClient(buildAddonBasePath(service));
      await client.post(`/clusters/${clusterId}:resume`);
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

  if (error || !result) {
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

  const sl = stateLabel(state, result.resource.state);

  return (
    <div className="ome-core-detail-layout">
      <PageHeader
        title={clusterName}
        subtitle={`Created ${formatTime(result.resource.createTime)}`}
        label={
          <Label
            color={sl.color}
            isCompact
            className="pf-v6-u-mr-sm"
            icon={state === "PAUSED_AUTH" ? <PauseCircleIcon /> : undefined}
          >
            {sl.text}
            {result.resource.reconciling ? " (reconciling)" : ""}
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
            <OverviewTab result={result} service={service} />
          </div>
        </Tab>
        {resolvedTabs
          .filter((tab) => !tab.service || tab.service === service)
          .map((tab) => (
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
