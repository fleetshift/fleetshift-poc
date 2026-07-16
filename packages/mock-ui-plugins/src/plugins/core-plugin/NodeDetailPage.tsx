import {
  createResourceApi,
  PluginLink,
  type ResourceResult,
} from "@fleetshift/common";
import { PageHeader } from "@patternfly/react-component-groups/dist/dynamic/PageHeader";
import {
  Breadcrumb,
  BreadcrumbItem,
  Card,
  CardBody,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  EmptyState,
  EmptyStateBody,
  EmptyStateFooter,
  Label,
  LabelGroup,
  Spinner,
  Title,
} from "@patternfly/react-core";
import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";

import { formatAge, type K8sObjectResource } from "./useClusterResources";

const k8sApi = createResourceApi<K8sObjectResource>("-");

function nodeStatusFromConditions(resource: Record<string, unknown>): {
  text: string;
  color: "green" | "red" | "grey";
} {
  const conditions = resource.conditions as
    | Record<string, { status: string }>
    | undefined;
  if (!conditions) return { text: "Unknown", color: "grey" };
  const ready = conditions["Ready"];
  if (ready?.status === "True") return { text: "Ready", color: "green" };
  if (ready?.status === "False") return { text: "NotReady", color: "red" };
  return { text: "Unknown", color: "grey" };
}

function formatBytes(bytes: unknown): string {
  if (typeof bytes !== "number" || bytes <= 0) return "—";
  const gb = bytes / (1024 * 1024 * 1024);
  if (gb >= 1) return `${gb.toFixed(1)} GiB`;
  const mb = bytes / (1024 * 1024);
  return `${mb.toFixed(0)} MiB`;
}

export default function NodeDetailPage() {
  const { clusterId, nodeUid } = useParams<{
    clusterId: string;
    nodeUid: string;
  }>();
  const [resource, setResource] =
    useState<ResourceResult<K8sObjectResource> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!clusterId || !nodeUid) return;
    let stale = false;
    setLoading(true);
    setError(null);
    k8sApi
      .get(
        `//kubernetes.fleetshift.io/clusters/${clusterId}/apiResources/core~v1~nodes/objects/${nodeUid}`,
      )
      .then((result) => {
        if (stale) return;
        if (result) {
          setResource(result);
        } else {
          setError("Node not found");
        }
      })
      .catch((e) => {
        if (stale) return;
        setError(e instanceof Error ? e.message : "Failed to load node");
      })
      .finally(() => {
        if (!stale) setLoading(false);
      });
    return () => {
      stale = true;
    };
  }, [clusterId, nodeUid]);

  if (loading) {
    return <Spinner aria-label="Loading node details" />;
  }

  if (error || !resource) {
    return (
      <EmptyState titleText={error ?? "Node not found"} headingLevel="h1">
        <EmptyStateBody>The requested node could not be loaded.</EmptyStateBody>
        <EmptyStateFooter>
          <PluginLink scope="core-plugin" module="NodesModule">
            Back to Nodes
          </PluginLink>
        </EmptyStateFooter>
      </EmptyState>
    );
  }

  const obs = resource.resource.observation;
  const meta = obs?.metadata;
  const extracted = obs?.extracted;
  const nodeName = meta?.name ?? nodeUid ?? "Unknown";
  const status = nodeStatusFromConditions(
    resource.resource as unknown as Record<string, unknown>,
  );
  const role = (extracted?.role as string) ?? "worker";
  const kubeletVersion = (extracted?.kubeletVersion as string) ?? "—";
  const osImage = (extracted?.osImage as string) ?? "—";
  const ipAddress = (extracted?.ipAddress as string) ?? "—";
  const memAlloc = extracted?.memoryAllocatable;
  const memCap = extracted?.memoryCapacity;
  const cpuAlloc = (extracted?.cpuAllocatable as string) ?? "—";
  const cpuCap = (extracted?.cpuCapacity as string) ?? "—";
  const labels = resource.resource.labels ?? {};
  const conditions = (resource.resource as Record<string, unknown>)
    .conditions as
    | Record<string, { status: string; reason?: string; message?: string }>
    | undefined;

  return (
    <div>
      <PageHeader
        title={nodeName}
        subtitle={`Role: ${role}`}
        label={
          <>
            <Label color={status.color} isCompact className="pf-v6-u-mr-sm">
              {status.text}
            </Label>
            <Label isCompact color="blue">
              Node
            </Label>
          </>
        }
        breadcrumbs={
          <Breadcrumb>
            <BreadcrumbItem
              render={({ className, ariaCurrent }) => (
                <PluginLink
                  scope="core-plugin"
                  module="NodesModule"
                  className={className}
                  aria-current={ariaCurrent}
                >
                  Nodes
                </PluginLink>
              )}
            />
            <BreadcrumbItem isActive>{nodeName}</BreadcrumbItem>
          </Breadcrumb>
        }
      />

      <Card className="pf-v6-u-mt-md">
        <CardBody>
          <Title headingLevel="h2" size="lg" className="pf-v6-u-mb-md">
            Node Information
          </Title>
          <DescriptionList isHorizontal>
            <DescriptionListGroup>
              <DescriptionListTerm>Status</DescriptionListTerm>
              <DescriptionListDescription>
                <Label color={status.color} isCompact>
                  {status.text}
                </Label>
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Roles</DescriptionListTerm>
              <DescriptionListDescription>{role}</DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Kubelet Version</DescriptionListTerm>
              <DescriptionListDescription>
                {kubeletVersion}
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>OS Image</DescriptionListTerm>
              <DescriptionListDescription>{osImage}</DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>IP Address</DescriptionListTerm>
              <DescriptionListDescription>
                {ipAddress}
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>CPU</DescriptionListTerm>
              <DescriptionListDescription>
                {cpuAlloc} allocatable / {cpuCap} capacity
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Memory</DescriptionListTerm>
              <DescriptionListDescription>
                {formatBytes(memAlloc)} allocatable / {formatBytes(memCap)}{" "}
                capacity
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>UID</DescriptionListTerm>
              <DescriptionListDescription>
                {meta?.uid ?? "—"}
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Age</DescriptionListTerm>
              <DescriptionListDescription>
                {formatAge(meta?.creationTimestamp)}
              </DescriptionListDescription>
            </DescriptionListGroup>
          </DescriptionList>
        </CardBody>
      </Card>

      {Object.keys(labels).length > 0 && (
        <Card className="pf-v6-u-mt-md">
          <CardBody>
            <Title headingLevel="h2" size="lg" className="pf-v6-u-mb-md">
              Labels
            </Title>
            <LabelGroup>
              {Object.entries(labels).map(([k, v]) => (
                <Label key={k} isCompact>
                  {k}={v}
                </Label>
              ))}
            </LabelGroup>
          </CardBody>
        </Card>
      )}

      {conditions && Object.keys(conditions).length > 0 && (
        <Card className="pf-v6-u-mt-md">
          <CardBody>
            <Title headingLevel="h2" size="lg" className="pf-v6-u-mb-md">
              Conditions
            </Title>
            <DescriptionList isHorizontal>
              {Object.entries(conditions).map(([type, cond]) => (
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
    </div>
  );
}
