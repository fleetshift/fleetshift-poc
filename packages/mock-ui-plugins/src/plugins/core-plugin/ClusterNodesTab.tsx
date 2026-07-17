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
import {
  DataViewTable,
  type DataViewTh,
  type DataViewTr,
} from "@patternfly/react-data-view/dist/dynamic/DataViewTable";
import { DataViewToolbar } from "@patternfly/react-data-view/dist/dynamic/DataViewToolbar";
import { useDataViewPagination } from "@patternfly/react-data-view/dist/dynamic/Hooks";
import { Tbody, Td, Tr } from "@patternfly/react-table";
import { useMemo } from "react";

import {
  extractClusterId,
  formatAge,
  useClusterResources,
} from "./useClusterResources";

const baseColumns: DataViewTh[] = [
  "Name",
  "Status",
  "Roles",
  "Version",
  "OS Image",
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
];

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

export default function ClusterNodesTab({
  clusterId,
}: {
  clusterId: string | undefined;
}) {
  const { resources, loading, error } = useClusterResources(
    clusterId,
    "core~v1~nodes",
  );

  const pagination = useDataViewPagination({ perPage: 10 });
  const { page, perPage } = pagination;

  const columns = clusterId ? baseColumns : allClustersColumns;

  const rows: DataViewTr[] = useMemo(
    () =>
      resources
        .slice((page - 1) * perPage, (page - 1) * perPage + perPage)
        .map((r) => {
          const meta = r.resource.observation?.metadata;
          const extracted = r.resource.observation?.extracted;
          const role = (extracted?.role as string) ?? "worker";
          const kubeletVersion = (extracted?.kubeletVersion as string) ?? "—";
          const osImage = (extracted?.osImage as string) ?? "—";
          const status = nodeStatusFromConditions(
            r.resource as unknown as Record<string, unknown>,
          );
          const memAlloc = extracted?.memoryAllocatable;
          const cpuAlloc = (extracted?.cpuAllocatable as string) ?? "—";
          const cid = clusterId ?? extractClusterId(r.name);

          const row: DataViewTr = [
            {
              cell: (
                <PluginLink
                  scope="core-plugin"
                  module="NodesModule"
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
            {
              cell: (
                <Label color={status.color} isCompact>
                  {status.text}
                </Label>
              ),
            },
            role,
            kubeletVersion,
            {
              cell: (
                <span>
                  {osImage}
                  {memAlloc
                    ? ` (${formatBytes(memAlloc)}, ${cpuAlloc} CPU)`
                    : ""}
                </span>
              ),
            },
            formatAge(meta?.creationTimestamp),
          ];
          return row;
        }),
    [resources, page, perPage, clusterId],
  );

  const activeState = loading
    ? DataViewState.loading
    : error
      ? "error"
      : resources.length === 0
        ? "empty"
        : undefined;

  return (
    <DataView activeState={activeState}>
      <DataViewToolbar
        pagination={
          <Pagination
            perPageOptions={PER_PAGE_OPTIONS}
            itemCount={resources.length}
            {...pagination}
          />
        }
      />
      <DataViewTable
        aria-label="Nodes table"
        columns={columns}
        rows={rows}
        headStates={{ loading: <SkeletonTableHead columns={columns} /> }}
        bodyStates={{
          loading: (
            <SkeletonTableBody rowsCount={3} columnsCount={columns.length} />
          ),
          empty: (
            <Tbody>
              <Tr>
                <Td colSpan={columns.length}>
                  <EmptyState headingLevel="h3" titleText="No nodes found">
                    <EmptyStateBody>
                      No nodes are currently indexed for this cluster.
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
                  <EmptyState
                    headingLevel="h3"
                    titleText="Unable to load nodes"
                  >
                    <EmptyStateBody>{error}</EmptyStateBody>
                  </EmptyState>
                </Td>
              </Tr>
            </Tbody>
          ),
        }}
      />
    </DataView>
  );
}
