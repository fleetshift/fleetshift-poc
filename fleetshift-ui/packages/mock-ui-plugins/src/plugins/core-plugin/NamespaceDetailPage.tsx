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

function phaseColor(phase: string | undefined): "green" | "red" | "grey" {
  if (!phase) return "grey";
  if (phase === "Active") return "green";
  if (phase === "Terminating") return "red";
  return "grey";
}

export default function NamespaceDetailPage() {
  const { clusterId, nsUid } = useParams<{
    clusterId: string;
    nsUid: string;
  }>();
  const [resource, setResource] =
    useState<ResourceResult<K8sObjectResource> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!clusterId || !nsUid) return;
    let stale = false;
    setLoading(true);
    setError(null);
    k8sApi
      .get(
        `//kubernetes.fleetshift.io/clusters/${clusterId}/apiResources/core~v1~namespaces/objects/${nsUid}`,
      )
      .then((result) => {
        if (stale) return;
        if (result) {
          setResource(result);
        } else {
          setError("Namespace not found");
        }
      })
      .catch((e) => {
        if (stale) return;
        setError(e instanceof Error ? e.message : "Failed to load namespace");
      })
      .finally(() => {
        if (!stale) setLoading(false);
      });
    return () => {
      stale = true;
    };
  }, [clusterId, nsUid]);

  if (loading) {
    return <Spinner aria-label="Loading namespace details" />;
  }

  if (error || !resource) {
    return (
      <EmptyState titleText={error ?? "Namespace not found"} headingLevel="h1">
        <EmptyStateBody>
          The requested namespace could not be loaded.
        </EmptyStateBody>
        <EmptyStateFooter>
          <PluginLink scope="core-plugin" module="NamespacesModule">
            Back to Namespaces
          </PluginLink>
        </EmptyStateFooter>
      </EmptyState>
    );
  }

  const obs = resource.resource.observation;
  const meta = obs?.metadata;
  const extracted = obs?.extracted;
  const phase = (extracted?.phase as string) ?? "";
  const nsName = meta?.name ?? nsUid ?? "Unknown";
  const labels = resource.resource.labels ?? {};
  const conditions = (resource.resource as Record<string, unknown>)
    .conditions as
    | Record<string, { status: string; reason?: string; message?: string }>
    | undefined;

  return (
    <div>
      <PageHeader
        title={nsName}
        label={
          <>
            <Label
              color={phaseColor(phase)}
              isCompact
              className="pf-v6-u-mr-sm"
            >
              {phase || "Unknown"}
            </Label>
            <Label isCompact color="blue">
              Namespace
            </Label>
          </>
        }
        breadcrumbs={
          <Breadcrumb>
            <BreadcrumbItem
              render={({ className, ariaCurrent }) => (
                <PluginLink
                  scope="core-plugin"
                  module="NamespacesModule"
                  className={className}
                  aria-current={ariaCurrent}
                >
                  Namespaces
                </PluginLink>
              )}
            />
            <BreadcrumbItem isActive>{nsName}</BreadcrumbItem>
          </Breadcrumb>
        }
      />

      <Card className="pf-v6-u-mt-md">
        <CardBody>
          <Title headingLevel="h2" size="lg" className="pf-v6-u-mb-md">
            Namespace Information
          </Title>
          <DescriptionList isHorizontal>
            <DescriptionListGroup>
              <DescriptionListTerm>Status</DescriptionListTerm>
              <DescriptionListDescription>
                <Label color={phaseColor(phase)} isCompact>
                  {phase || "Unknown"}
                </Label>
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
