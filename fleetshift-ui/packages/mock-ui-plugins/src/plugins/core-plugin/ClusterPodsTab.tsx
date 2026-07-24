import { PluginLink } from "@fleetshift/common";
import {
  SkeletonTableBody,
  SkeletonTableHead,
} from "@patternfly/react-component-groups";
import {
  EmptyState,
  EmptyStateBody,
  Label,
  Pagination,
} from "@patternfly/react-core";
import {
  DataView,
  DataViewState,
} from "@patternfly/react-data-view/dist/dynamic/DataView";
import { DataViewFilters } from "@patternfly/react-data-view/dist/dynamic/DataViewFilters";
import {
  DataViewTable,
  type DataViewTh,
  type DataViewTr,
} from "@patternfly/react-data-view/dist/dynamic/DataViewTable";
import { DataViewTextFilter } from "@patternfly/react-data-view/dist/dynamic/DataViewTextFilter";
import { DataViewToolbar } from "@patternfly/react-data-view/dist/dynamic/DataViewToolbar";
import {
  useDataViewFilters,
  useDataViewPagination,
} from "@patternfly/react-data-view/dist/dynamic/Hooks";
import { Tbody, Td, Tr } from "@patternfly/react-table";
import { useMemo } from "react";

import {
  extractClusterId,
  formatAge,
  useClusterResources,
} from "./useClusterResources";

interface PodFilters {
  name: string;
  namespace: string;
}

const baseColumns: DataViewTh[] = [
  "Name",
  "Namespace",
  "Status",
  "Node",
  "Restarts",
  "Age",
];
const allClustersColumns: DataViewTh[] = [
  "Name",
  "Cluster",
  ...baseColumns.slice(1),
];

const PER_PAGE_OPTIONS = [
  { title: "10", value: 10 },
  { title: "25", value: 25 },
  { title: "50", value: 50 },
];

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

export default function ClusterPodsTab({
  clusterId,
}: {
  clusterId: string | undefined;
}) {
  const { resources, loading, error } = useClusterResources(
    clusterId,
    "core~v1~pods",
  );

  const { filters, onSetFilters, clearAllFilters } =
    useDataViewFilters<PodFilters>({
      initialFilters: { name: "", namespace: "" },
    });
  const pagination = useDataViewPagination({ perPage: 10 });
  const { page, perPage } = pagination;

  const filtered = useMemo(
    () =>
      resources.filter((r) => {
        const meta = r.resource.observation?.metadata;
        if (!meta) return false;
        if (
          filters.name &&
          !meta.name.toLowerCase().includes(filters.name.toLowerCase())
        )
          return false;
        if (
          filters.namespace &&
          !meta.namespace
            .toLowerCase()
            .includes(filters.namespace.toLowerCase())
        )
          return false;
        return true;
      }),
    [resources, filters],
  );

  const columns = clusterId ? baseColumns : allClustersColumns;

  const rows: DataViewTr[] = useMemo(
    () =>
      filtered
        .slice((page - 1) * perPage, (page - 1) * perPage + perPage)
        .map((r) => {
          const meta = r.resource.observation?.metadata;
          const extracted = r.resource.observation?.extracted;
          const status = (extracted?.status as string) ?? "";
          const nodeName = (extracted?.nodeName as string) ?? "—";
          const restartCounts = (extracted?.restartCount as number[]) ?? [];
          const totalRestarts = restartCounts.reduce(
            (sum, c) => sum + (c ?? 0),
            0,
          );
          const cid = clusterId ?? extractClusterId(r.name);

          const row: DataViewTr = [
            {
              cell: (
                <PluginLink
                  scope="core-plugin"
                  module="PodsModule"
                  to={`${cid}/${meta?.uid}`}
                >
                  {meta?.name ?? "—"}
                </PluginLink>
              ),
            },
            ...(clusterId
              ? []
              : [
                  {
                    cell: (
                      <PluginLink
                        scope="core-plugin"
                        module="ClustersModule"
                        to={cid}
                      >
                        {cid}
                      </PluginLink>
                    ),
                  },
                ]),
            meta?.namespace ?? "—",
            {
              cell: (
                <Label color={podStatusColor(status)} isCompact>
                  {status || "Unknown"}
                </Label>
              ),
            },
            nodeName,
            totalRestarts,
            formatAge(meta?.creationTimestamp),
          ];
          return row;
        }),
    [filtered, page, perPage, clusterId],
  );

  const activeState = loading
    ? DataViewState.loading
    : error
      ? "error"
      : filtered.length === 0
        ? "empty"
        : undefined;

  return (
    <DataView activeState={activeState}>
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
            <DataViewTextFilter
              filterId="namespace"
              title="Namespace"
              placeholder="Filter by namespace"
            />
          </DataViewFilters>
        }
      />
      <DataViewTable
        aria-label="Pods table"
        columns={columns}
        rows={rows}
        headStates={{ loading: <SkeletonTableHead columns={columns} /> }}
        bodyStates={{
          loading: (
            <SkeletonTableBody rowsCount={5} columnsCount={columns.length} />
          ),
          empty: (
            <Tbody>
              <Tr>
                <Td colSpan={columns.length}>
                  <EmptyState headingLevel="h3" titleText="No pods found">
                    <EmptyStateBody>
                      No pods are currently indexed for this cluster.
                    </EmptyStateBody>
                  </EmptyState>
                </Td>
              </Tr>
            </Tbody>
          ),
          error: (
            <Tbody>
              <Tr>
                <Td colSpan={columns.length}>
                  <EmptyState headingLevel="h3" titleText="Unable to load pods">
                    <EmptyStateBody>{error}</EmptyStateBody>
                  </EmptyState>
                </Td>
              </Tr>
            </Tbody>
          ),
        }}
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
  );
}
