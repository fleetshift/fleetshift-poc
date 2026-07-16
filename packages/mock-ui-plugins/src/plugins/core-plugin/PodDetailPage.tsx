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

function podStatusColor(
  status: string | undefined,
): "green" | "blue" | "orange" | "red" | "grey" {
  if (!status) return "grey";
  const s = status.toLowerCase();
  if (s === "running") return "green";
  if (s === "succeeded" || s === "completed") return "blue";
  if (s.startsWith("init:") || s === "pending" || s === "containercreating")
    return "orange";
  if (
    s === "failed" ||
    s === "crashloopbackoff" ||
    s === "error" ||
    s === "imagepullbackoff"
  )
    return "red";
  return "grey";
}

export default function PodDetailPage() {
  const { clusterId, podUid } = useParams<{
    clusterId: string;
    podUid: string;
  }>();
  const [resource, setResource] =
    useState<ResourceResult<K8sObjectResource> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!clusterId || !podUid) return;
    setLoading(true);
    setError(null);
    k8sApi
      .get(
        `//kubernetes.fleetshift.io/clusters/${clusterId}/apiResources/core~v1~pods/objects/${podUid}`,
      )
      .then((result) => {
        if (result) {
          setResource(result);
        } else {
          setError("Pod not found");
        }
      })
      .catch((e) => {
        setError(e instanceof Error ? e.message : "Failed to load pod");
      })
      .finally(() => setLoading(false));
  }, [clusterId, podUid]);

  if (loading) {
    return <Spinner aria-label="Loading pod details" />;
  }

  if (error || !resource) {
    return (
      <EmptyState titleText={error ?? "Pod not found"} headingLevel="h1">
        <EmptyStateBody>The requested pod could not be loaded.</EmptyStateBody>
        <EmptyStateFooter>
          <PluginLink scope="core-plugin" module="PodsModule">
            Back to Pods
          </PluginLink>
        </EmptyStateFooter>
      </EmptyState>
    );
  }

  const obs = resource.resource.observation;
  const meta = obs?.metadata;
  const extracted = obs?.extracted;
  const status = (extracted?.status as string) ?? "";
  const podName = meta?.name ?? podUid ?? "Unknown";
  const namespace = meta?.namespace ?? "—";
  const nodeName = (extracted?.nodeName as string) ?? "—";
  const podIP = (extracted?.podIP as string) ?? "—";
  const hostIP = (extracted?.hostIP as string) ?? "—";
  const containerImages = (extracted?.containerImages as string[]) ?? [];
  const restartCounts = (extracted?.restartCount as number[]) ?? [];
  const totalRestarts = restartCounts.reduce((sum, c) => sum + (c ?? 0), 0);
  const labels = resource.resource.labels ?? {};
  const conditions = (resource.resource as Record<string, unknown>)
    .conditions as
    | Record<string, { status: string; reason?: string; message?: string }>
    | undefined;

  return (
    <div>
      <PageHeader
        title={podName}
        subtitle={`Namespace: ${namespace}`}
        label={
          <>
            <Label
              color={podStatusColor(status)}
              isCompact
              className="pf-v6-u-mr-sm"
            >
              {status || "Unknown"}
            </Label>
            <Label isCompact color="blue">
              Pod
            </Label>
          </>
        }
        breadcrumbs={
          <Breadcrumb>
            <BreadcrumbItem
              render={({ className, ariaCurrent }) => (
                <PluginLink
                  scope="core-plugin"
                  module="PodsModule"
                  className={className}
                  aria-current={ariaCurrent}
                >
                  Pods
                </PluginLink>
              )}
            />
            <BreadcrumbItem isActive>{podName}</BreadcrumbItem>
          </Breadcrumb>
        }
      />

      <Card className="pf-v6-u-mt-md">
        <CardBody>
          <Title headingLevel="h2" size="lg" className="pf-v6-u-mb-md">
            Pod Information
          </Title>
          <DescriptionList isHorizontal>
            <DescriptionListGroup>
              <DescriptionListTerm>Namespace</DescriptionListTerm>
              <DescriptionListDescription>
                {namespace}
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Status</DescriptionListTerm>
              <DescriptionListDescription>
                <Label color={podStatusColor(status)} isCompact>
                  {status || "Unknown"}
                </Label>
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Node</DescriptionListTerm>
              <DescriptionListDescription>
                {nodeName}
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Pod IP</DescriptionListTerm>
              <DescriptionListDescription>{podIP}</DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Host IP</DescriptionListTerm>
              <DescriptionListDescription>{hostIP}</DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Restarts</DescriptionListTerm>
              <DescriptionListDescription>
                {totalRestarts}
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

      {containerImages.length > 0 && (
        <Card className="pf-v6-u-mt-md">
          <CardBody>
            <Title headingLevel="h2" size="lg" className="pf-v6-u-mb-md">
              Containers
            </Title>
            <DescriptionList>
              {containerImages.map((image, idx) => (
                <DescriptionListGroup key={idx}>
                  <DescriptionListTerm>Image {idx + 1}</DescriptionListTerm>
                  <DescriptionListDescription>
                    {image}
                    {restartCounts[idx] != null && restartCounts[idx] > 0 && (
                      <Label isCompact color="orange" className="pf-v6-u-ml-sm">
                        {restartCounts[idx]} restarts
                      </Label>
                    )}
                  </DescriptionListDescription>
                </DescriptionListGroup>
              ))}
            </DescriptionList>
          </CardBody>
        </Card>
      )}

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
