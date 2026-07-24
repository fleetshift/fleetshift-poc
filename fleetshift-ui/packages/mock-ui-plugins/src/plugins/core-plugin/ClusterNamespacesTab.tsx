import { PluginLink } from "@fleetshift/common";
import {
  SkeletonTableBody,
  SkeletonTableHead,
} from "@patternfly/react-component-groups";
import {
  EmptyState,
  EmptyStateBody,
  Label,
  LabelGroup,
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

interface NamespaceFilters {
  name: string;
}

const baseColumns: DataViewTh[] = ["Name", "Status", "Labels", "Age"];
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

function phaseColor(phase: string | undefined): "green" | "red" | "grey" {
  if (!phase) return "grey";
  if (phase === "Active") return "green";
  if (phase === "Terminating") return "red";
  return "grey";
}

export default function ClusterNamespacesTab({
  clusterId,
}: {
  clusterId: string | undefined;
}) {
  const { resources, loading, error } = useClusterResources(
    clusterId,
    "core~v1~namespaces",
  );

  const { filters, onSetFilters, clearAllFilters } =
    useDataViewFilters<NamespaceFilters>({
      initialFilters: { name: "" },
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
          const phase = (extracted?.phase as string) ?? "";
          const labels = r.resource.labels ?? {};
          const labelEntries = Object.entries(labels).slice(0, 5);
          const cid = clusterId ?? extractClusterId(r.name);

          const row: DataViewTr = [
            {
              cell: (
                <PluginLink
                  scope="core-plugin"
                  module="NamespacesModule"
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
                <Label color={phaseColor(phase)} isCompact>
                  {phase || "Unknown"}
                </Label>
              ),
            },
            {
              cell:
                labelEntries.length > 0 ? (
                  <LabelGroup>
                    {labelEntries.map(([k, v]) => (
                      <Label key={k} isCompact>
                        {k}={v}
                      </Label>
                    ))}
                    {Object.keys(labels).length > 5 && (
                      <Label isCompact>
                        +{Object.keys(labels).length - 5} more
                      </Label>
                    )}
                  </LabelGroup>
                ) : (
                  "—"
                ),
            },
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
          </DataViewFilters>
        }
      />
      <DataViewTable
        aria-label="Namespaces table"
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
                  <EmptyState headingLevel="h3" titleText="No namespaces found">
                    <EmptyStateBody>
                      No namespaces are currently indexed for this cluster.
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
                    titleText="Unable to load namespaces"
                  >
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
