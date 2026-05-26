import { useState, useEffect, useCallback, useMemo } from "react";
import { Link } from "react-router-dom";
import {
  Card,
  CardBody,
  Content,
  Grid,
  GridItem,
  Label,
  Pagination,
  Spinner,
  Stack,
  StackItem,
  Title,
} from "@patternfly/react-core";
import { ActionsColumn } from "@patternfly/react-table";
import {
  useDataViewFilters,
  useDataViewPagination,
} from "@patternfly/react-data-view/dist/dynamic/Hooks";
import { DataView } from "@patternfly/react-data-view/dist/dynamic/DataView";
import {
  DataViewTable,
  type DataViewTr,
  type DataViewTh,
} from "@patternfly/react-data-view/dist/dynamic/DataViewTable";
import { DataViewToolbar } from "@patternfly/react-data-view/dist/dynamic/DataViewToolbar";
import { DataViewFilters } from "@patternfly/react-data-view/dist/dynamic/DataViewFilters";
import { DataViewTextFilter } from "@patternfly/react-data-view/dist/dynamic/DataViewTextFilter";

import {
  listDeployments,
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

interface ClusterRow {
  deployment: MgmtDeployment;
  clusterName: string;
  nodeCount: number;
  spec: KindClusterSpec | null;
}

interface ClusterFilters {
  name: string;
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

function toClusterRow(dep: MgmtDeployment): ClusterRow {
  const spec = decodeSpec(dep);
  const shortName = dep.name.replace(/^deployments\//, "");
  return {
    deployment: dep,
    clusterName: spec?.name ?? shortName,
    nodeCount: spec?.nodes?.length ?? 0,
    spec,
  };
}

function formatTime(iso: string): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

const columns: DataViewTh[] = [
  "Name",
  "Type",
  "Status",
  "Nodes",
  "Created",
  "",
];

const PER_PAGE_OPTIONS = [
  { title: "10", value: 10 },
  { title: "25", value: 25 },
  { title: "50", value: 50 },
];

export default function ClustersPage() {
  const [rows, setRows] = useState<ClusterRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);

  const { filters, onSetFilters, clearAllFilters } =
    useDataViewFilters<ClusterFilters>({ initialFilters: { name: "" } });
  const pagination = useDataViewPagination({ perPage: 10 });
  const { page, perPage } = pagination;

  const fetchClusters = useCallback(async () => {
    try {
      const resp = await listDeployments();
      const clusterDeps = (resp.deployments ?? []).filter((d) =>
        d.manifestStrategy?.manifests?.some(
          (m) => m.resourceType === "api.kind.cluster",
        ),
      );
      setRows(clusterDeps.map(toClusterRow));
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load clusters");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchClusters();
  }, [fetchClusters]);

  const filtered = useMemo(
    () =>
      rows.filter(
        (r) =>
          !filters.name ||
          r.clusterName.toLowerCase().includes(filters.name.toLowerCase()),
      ),
    [rows, filters],
  );

  const pageRows: DataViewTr[] = useMemo(
    () =>
      filtered
        .slice((page - 1) * perPage, (page - 1) * perPage + perPage)
        .map((r) => {
          const depName = r.deployment.name.replace(/^deployments\//, "");
          const stateLabel =
            STATE_LABELS[r.deployment.state] ?? STATE_LABELS.STATE_UNSPECIFIED;
          const isDeleting = deleting === depName;

          return [
            {
              cell: (
                <Link to={depName}>
                  <strong>{r.clusterName}</strong>
                </Link>
              ),
            },
            {
              cell: (
                <Label color="blue" isCompact>
                  Kind
                </Label>
              ),
            },
            {
              cell: (
                <Label color={stateLabel.color} isCompact>
                  {stateLabel.text}
                  {r.deployment.reconciling ? " (reconciling)" : ""}
                </Label>
              ),
            },
            r.nodeCount,
            formatTime(r.deployment.createTime),
            {
              cell:
                r.deployment.state !== "STATE_DELETING" ? (
                  <ActionsColumn
                    items={[
                      {
                        title: "Delete",
                        onClick: () => handleDelete(depName),
                        isDisabled: isDeleting,
                      },
                    ]}
                  />
                ) : null,
              props: { isActionCell: true },
            },
          ];
        }),
    [filtered, page, perPage, deleting],
  );

  const totalNodes = rows.reduce((sum, r) => sum + r.nodeCount, 0);
  const healthy = rows.filter(
    (r) => r.deployment.state === "STATE_ACTIVE",
  ).length;
  const needsAttention = rows.length - healthy;

  const handleDelete = async (name: string) => {
    setDeleting(name);
    try {
      await deleteDeployment(name);
      await fetchClusters();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Delete failed");
    } finally {
      setDeleting(null);
    }
  };

  if (loading) {
    return <Spinner aria-label="Loading clusters" />;
  }

  return (
    <Stack hasGutter>
      <StackItem>
        <div>
          <Title headingLevel="h1">Clusters</Title>
          <Content component="p">
            Manage and monitor your OpenShift clusters
          </Content>
        </div>
      </StackItem>

      {error && (
        <StackItem>
          <Content
            component="p"
            style={{
              color: "var(--pf-t--global--color--status--danger--default)",
            }}
          >
            {error}
          </Content>
        </StackItem>
      )}

      <StackItem>
        <Grid hasGutter>
          <GridItem span={3}>
            <Card isPlain isCompact>
              <CardBody>
                <Content component="p">Total Clusters</Content>
                <Title headingLevel="h2" size="2xl">
                  {rows.length}
                </Title>
              </CardBody>
            </Card>
          </GridItem>
          <GridItem span={3}>
            <Card isPlain isCompact>
              <CardBody>
                <Content component="p">Total Nodes</Content>
                <Title headingLevel="h2" size="2xl">
                  {totalNodes}
                </Title>
              </CardBody>
            </Card>
          </GridItem>
          <GridItem span={3}>
            <Card isPlain isCompact>
              <CardBody>
                <Content component="p">Healthy</Content>
                <Title
                  headingLevel="h2"
                  size="2xl"
                  style={{
                    color:
                      "var(--pf-t--global--color--status--success--default)",
                  }}
                >
                  {healthy}
                </Title>
              </CardBody>
            </Card>
          </GridItem>
          <GridItem span={3}>
            <Card isPlain isCompact>
              <CardBody>
                <Content component="p">Needs Attention</Content>
                <Title
                  headingLevel="h2"
                  size="2xl"
                  style={{
                    color:
                      needsAttention > 0
                        ? "var(--pf-t--global--color--status--danger--default)"
                        : undefined,
                  }}
                >
                  {needsAttention}
                </Title>
              </CardBody>
            </Card>
          </GridItem>
        </Grid>
      </StackItem>

      <StackItem>
        <DataView>
          <DataViewToolbar
            clearAllFilters={clearAllFilters}
            pagination={
              <Pagination
                perPageOptions={PER_PAGE_OPTIONS}
                itemCount={filtered.length}
                {...pagination}
              />
            }
            filters={
              <DataViewFilters
                onChange={(_e, values) => onSetFilters(values)}
                values={filters}
              >
                <DataViewTextFilter
                  filterId="name"
                  title="Name"
                  placeholder="Filter by name"
                />
              </DataViewFilters>
            }
          />
          <DataViewTable
            aria-label="Clusters table"
            columns={columns}
            rows={pageRows}
          />
          <DataViewToolbar
            pagination={
              <Pagination
                isCompact
                perPageOptions={PER_PAGE_OPTIONS}
                itemCount={filtered.length}
                {...pagination}
              />
            }
          />
        </DataView>
      </StackItem>
    </Stack>
  );
}
