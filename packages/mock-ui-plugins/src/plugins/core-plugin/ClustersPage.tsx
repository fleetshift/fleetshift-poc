import "./ClustersPage.css";

import {
  createApiClient,
  createResourceApi,
  PluginLink,
} from "@fleetshift/common";
import {
  SkeletonTableBody,
  SkeletonTableHead,
} from "@patternfly/react-component-groups";
import {
  Button,
  Content,
  EmptyState,
  EmptyStateActions,
  EmptyStateBody,
  EmptyStateFooter,
  Label,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  Pagination,
  Stack,
  StackItem,
  Title,
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
import { CubesIcon } from "@patternfly/react-icons";
import { ActionsColumn, Tbody, Td, Tr } from "@patternfly/react-table";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

import ClusterSummaryCards from "./ClusterSummaryCards";
import {
  buildAddonBasePath,
  type ClusterResource,
  type ClusterRow,
  extractClusterId,
  extractService,
  formatTime,
  isTransientState,
  serviceLabel,
  stateLabel,
} from "./clusterTypes";
import CreateClusterModal from "./CreateClusterModal";

interface ClusterFilters {
  name: string;
}

const columns: DataViewTh[] = [
  "Name",
  "Provider",
  "Status",
  "Version",
  "Node Pools",
  "Created",
  "",
];

const PER_PAGE_OPTIONS = [
  { title: "10", value: 10 },
  { title: "25", value: 25 },
  { title: "50", value: 50 },
];

const clusterApi = createResourceApi<ClusterResource>("-");

export default function ClustersPage() {
  const [rows, setRows] = useState<ClusterRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<ClusterRow | null>(null);
  const [silentFailCount, setSilentFailCount] = useState(0);

  const [searchParams, setSearchParams] = useSearchParams();
  const createParam = searchParams.get("create");
  const wizardOpen = createParam !== null;
  const preselectedProvider =
    createParam && createParam !== "" ? createParam : null;

  const openCreateWizard = useCallback(() => {
    setSearchParams((prev) => {
      prev.set("create", "");
      return prev;
    });
  }, [setSearchParams]);

  const selectProvider = useCallback(
    (providerId: string) => {
      setSearchParams((prev) => {
        prev.set("create", providerId);
        return prev;
      });
    },
    [setSearchParams],
  );

  const { filters, onSetFilters, clearAllFilters } =
    useDataViewFilters<ClusterFilters>({ initialFilters: { name: "" } });
  const pagination = useDataViewPagination({ perPage: 10 });
  const { page, perPage } = pagination;

  const fetchClusters = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    try {
      const results = await clusterApi.searchAll({
        filter:
          'resourceType == "gcphcp.fleetshift.io/Cluster" || resourceType == "kind.fleetshift.io/Cluster"',
      });
      setRows(
        results.map((r) => ({
          result: r,
          id: extractClusterId(r.resource.name),
          service: extractService(r.name),
          nodePoolCount: Array.isArray(r.resource.spec?.nodepools)
            ? r.resource.spec.nodepools.length
            : 0,
        })),
      );
      setError(null);
      if (silent) setSilentFailCount(0);
    } catch (e) {
      if (silent) {
        setSilentFailCount((c) => c + 1);
      } else {
        setError(e instanceof Error ? e.message : "Failed to load clusters");
      }
    } finally {
      if (!silent) setLoading(false);
    }
  }, []);

  const closeCreateWizard = useCallback(() => {
    setSearchParams((prev) => {
      prev.delete("create");
      return prev;
    });
    fetchClusters(true);
  }, [setSearchParams, fetchClusters]);

  useEffect(() => {
    fetchClusters();
  }, [fetchClusters]);

  const hasTransient = rows.some(
    (r) =>
      isTransientState(r.result.resource.state) ||
      r.result.resource.reconciling,
  );
  useEffect(() => {
    if (!hasTransient || silentFailCount >= 3) return;
    const id = setInterval(() => fetchClusters(true), 5000);
    return () => clearInterval(id);
  }, [hasTransient, silentFailCount, fetchClusters]);

  const filtered = useMemo(
    () =>
      rows.filter(
        (r) =>
          !filters.name ||
          r.id.toLowerCase().includes(filters.name.toLowerCase()),
      ),
    [rows, filters],
  );

  const deleteTargetRef = useRef(deleteTarget);
  deleteTargetRef.current = deleteTarget;

  const handleDelete = async () => {
    const target = deleteTargetRef.current;
    if (!target) return;
    setDeleting(target.id);
    setDeleteTarget(null);
    try {
      const client = createApiClient(buildAddonBasePath(target.service));
      await client.delete(`/clusters/${target.id}`);
      await fetchClusters();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Delete failed");
    } finally {
      setDeleting(null);
    }
  };

  const pageRows: DataViewTr[] = useMemo(
    () =>
      filtered
        .slice((page - 1) * perPage, (page - 1) * perPage + perPage)
        .map((r) => {
          const sl = stateLabel(r.result.resource.state);
          const isDeleting = deleting === r.id;
          return [
            {
              cell: (
                <PluginLink
                  scope="core-plugin"
                  module="ClustersModule"
                  to={r.id}
                >
                  <strong>{r.id}</strong>
                </PluginLink>
              ),
            },
            {
              cell: (
                <Label color="blue" isCompact>
                  {serviceLabel(r.service)}
                </Label>
              ),
            },
            {
              cell: (
                <Label color={sl.color} isCompact>
                  {sl.text}
                  {r.result.resource.reconciling ? " (reconciling)" : ""}
                </Label>
              ),
            },
            r.result.resource.spec?.releaseVersion || "—",
            r.nodePoolCount,
            formatTime(r.result.resource.createTime),
            {
              cell:
                r.result.resource.state !== "DELETING" ? (
                  <ActionsColumn
                    items={[
                      {
                        title: isDeleting ? "Deleting..." : "Delete",
                        onClick: () => setDeleteTarget(r),
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
  const activeState = loading
    ? DataViewState.loading
    : error
      ? "error"
      : filtered.length === 0
        ? "empty"
        : undefined;

  const emptyBody = (
    <Tbody>
      <Tr>
        <Td colSpan={columns.length}>
          <EmptyState
            headingLevel="h2"
            icon={CubesIcon}
            titleText="No clusters found"
          >
            <EmptyStateBody>
              {rows.length === 0
                ? "Get started by creating your first cluster."
                : "No clusters match the current filter criteria."}
            </EmptyStateBody>
            <EmptyStateFooter>
              <EmptyStateActions>
                {rows.length === 0 ? (
                  <Button variant="primary" onClick={openCreateWizard}>
                    Create cluster
                  </Button>
                ) : (
                  <Button variant="link" onClick={clearAllFilters}>
                    Clear filters
                  </Button>
                )}
              </EmptyStateActions>
            </EmptyStateFooter>
          </EmptyState>
        </Td>
      </Tr>
    </Tbody>
  );

  const errorBody = (
    <Tbody>
      <Tr>
        <Td colSpan={columns.length}>
          <EmptyState headingLevel="h2" titleText="Unable to load clusters">
            <EmptyStateBody>{error}</EmptyStateBody>
            <EmptyStateFooter>
              <EmptyStateActions>
                <Button variant="primary" onClick={() => fetchClusters()}>
                  Try again
                </Button>
              </EmptyStateActions>
            </EmptyStateFooter>
          </EmptyState>
        </Td>
      </Tr>
    </Tbody>
  );

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

      <StackItem>
        <ClusterSummaryCards rows={rows} />
      </StackItem>

      <StackItem>
        <DataView activeState={activeState}>
          <DataViewToolbar
            clearAllFilters={clearAllFilters}
            className="ome-core-clusters-toolbar"
            actions={
              <Button variant="primary" onClick={openCreateWizard}>
                Create cluster
              </Button>
            }
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
            headStates={{
              loading: <SkeletonTableHead columns={columns} />,
            }}
            bodyStates={{
              loading: (
                <SkeletonTableBody
                  rowsCount={5}
                  columnsCount={columns.length}
                />
              ),
              empty: emptyBody,
              error: errorBody,
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
      </StackItem>

      <Modal
        isOpen={deleteTarget !== null}
        onClose={() => setDeleteTarget(null)}
        variant="small"
      >
        <ModalHeader
          title="Delete cluster"
          description={`Are you sure you want to delete "${deleteTarget?.id}"? This will terminate the provisioned cluster.`}
        />
        <ModalBody />
        <ModalFooter>
          <Button variant="danger" onClick={handleDelete}>
            Delete
          </Button>
          <Button variant="link" onClick={() => setDeleteTarget(null)}>
            Cancel
          </Button>
        </ModalFooter>
      </Modal>

      <CreateClusterModal
        isOpen={wizardOpen}
        preselectedProvider={preselectedProvider}
        onClose={closeCreateWizard}
        onProviderSelect={selectProvider}
      />
    </Stack>
  );
}
