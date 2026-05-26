import { useState, useMemo } from "react";
import {
  Bullseye,
  EmptyState,
  EmptyStateBody,
  Flex,
  FlexItem,
  Label,
  LabelGroup,
  Pagination,
  SearchInput,
  Spinner,
  Tab,
  Tabs,
  TabTitleText,
  Title,
  Toolbar,
  ToolbarContent,
  ToolbarItem,
} from "@patternfly/react-core";
import { Table, Thead, Tbody, Tr, Th, Td } from "@patternfly/react-table";
import { parseCapacity, accessModeLabel } from "@fleetshift/common";
import { usePVStore } from "./pvStore";
import { usePVCStore } from "./pvcStore";

const PER_PAGE = 20;

function pvStatusColor(status: string): "green" | "orange" | "red" | "grey" {
  switch (status) {
    case "Available":
    case "Bound":
      return "green";
    case "Released":
      return "orange";
    case "Failed":
      return "red";
    default:
      return "grey";
  }
}

function pvcStatusColor(status: string): "green" | "orange" | "red" | "grey" {
  switch (status) {
    case "Bound":
      return "green";
    case "Pending":
      return "orange";
    case "Lost":
      return "red";
    default:
      return "grey";
  }
}

const PVTab: React.FC = () => {
  const { pvs, loading } = usePVStore();
  const [nameFilter, setNameFilter] = useState("");
  const [page, setPage] = useState(1);

  const filtered = useMemo(
    () =>
      pvs.filter((pv) =>
        nameFilter
          ? pv.name.toLowerCase().includes(nameFilter.toLowerCase())
          : true,
      ),
    [pvs, nameFilter],
  );

  const paginatedItems = useMemo(
    () => filtered.slice((page - 1) * PER_PAGE, page * PER_PAGE),
    [filtered, page],
  );

  if (loading) {
    return (
      <Bullseye>
        <Spinner />
      </Bullseye>
    );
  }

  return (
    <>
      <Toolbar>
        <ToolbarContent>
          <ToolbarItem>
            <SearchInput
              placeholder="Filter by name"
              value={nameFilter}
              onChange={(_event, value) => {
                setNameFilter(value);
                setPage(1);
              }}
              onClear={() => {
                setNameFilter("");
                setPage(1);
              }}
            />
          </ToolbarItem>
          <ToolbarItem variant="pagination" align={{ default: "alignEnd" }}>
            <Pagination
              itemCount={filtered.length}
              perPage={PER_PAGE}
              page={page}
              onSetPage={(_event, p) => setPage(p)}
              isCompact
            />
          </ToolbarItem>
        </ToolbarContent>
      </Toolbar>

      {filtered.length === 0 ? (
        <EmptyState titleText="No persistent volumes found" headingLevel="h2">
          <EmptyStateBody>
            {pvs.length > 0
              ? "No persistent volumes match the current filter."
              : "There are no persistent volumes available."}
          </EmptyStateBody>
        </EmptyState>
      ) : (
        <>
          <Table
            aria-label="Persistent Volumes"
            variant="compact"
            hasAnimations
          >
            <Thead>
              <Tr>
                <Th>Name</Th>
                <Th>Capacity</Th>
                <Th>Access Modes</Th>

                <Th>Status</Th>
                <Th>Storage Class</Th>
                <Th>Cluster</Th>
              </Tr>
            </Thead>
            <Tbody>
              {paginatedItems.map((pv) => {
                const modes = pv.access_mode
                  .split(",")
                  .map((m) => m.trim())
                  .filter(Boolean);
                return (
                  <Tr key={pv.id}>
                    <Td dataLabel="Name">
                      <span
                        style={{
                          fontWeight:
                            "var(--pf-t--global--font--weight--heading--default)",
                        }}
                      >
                        {pv.name}
                      </span>
                    </Td>
                    <Td dataLabel="Capacity">
                      <Label color="blue" isCompact>
                        {parseCapacity(pv.capacity).value}{" "}
                        {parseCapacity(pv.capacity).unit}
                      </Label>
                    </Td>
                    <Td dataLabel="Access Modes">
                      <LabelGroup>
                        {modes.map((mode) => (
                          <Label key={mode} color="blue" isCompact>
                            {accessModeLabel(mode)}
                          </Label>
                        ))}
                      </LabelGroup>
                    </Td>
                    <Td dataLabel="Status">
                      <Label color={pvStatusColor(pv.status)} isCompact>
                        {pv.status}
                      </Label>
                    </Td>
                    <Td dataLabel="Storage Class">
                      <span
                        style={{
                          fontSize: "var(--pf-t--global--font--size--sm)",
                          color: "var(--pf-t--global--text--color--subtle)",
                        }}
                      >
                        {pv.storage_class || "\u2014"}
                      </span>
                    </Td>
                    <Td dataLabel="Cluster">{pv.cluster_id}</Td>
                  </Tr>
                );
              })}
            </Tbody>
          </Table>
          <Toolbar>
            <ToolbarContent>
              <ToolbarItem variant="pagination" align={{ default: "alignEnd" }}>
                <Pagination
                  itemCount={filtered.length}
                  perPage={PER_PAGE}
                  page={page}
                  onSetPage={(_event, p) => setPage(p)}
                  isCompact
                />
              </ToolbarItem>
            </ToolbarContent>
          </Toolbar>
        </>
      )}
    </>
  );
};

const PVCTab: React.FC = () => {
  const { pvcs, loading } = usePVCStore();
  const [nameFilter, setNameFilter] = useState("");
  const [page, setPage] = useState(1);

  const filtered = useMemo(
    () =>
      pvcs.filter((pvc) =>
        nameFilter
          ? pvc.name.toLowerCase().includes(nameFilter.toLowerCase())
          : true,
      ),
    [pvcs, nameFilter],
  );

  const paginatedItems = useMemo(
    () => filtered.slice((page - 1) * PER_PAGE, page * PER_PAGE),
    [filtered, page],
  );

  if (loading) {
    return (
      <Bullseye>
        <Spinner />
      </Bullseye>
    );
  }

  return (
    <>
      <Toolbar>
        <ToolbarContent>
          <ToolbarItem>
            <SearchInput
              placeholder="Filter by name"
              value={nameFilter}
              onChange={(_event, value) => {
                setNameFilter(value);
                setPage(1);
              }}
              onClear={() => {
                setNameFilter("");
                setPage(1);
              }}
            />
          </ToolbarItem>
          <ToolbarItem variant="pagination" align={{ default: "alignEnd" }}>
            <Pagination
              itemCount={filtered.length}
              perPage={PER_PAGE}
              page={page}
              onSetPage={(_event, p) => setPage(p)}
              isCompact
            />
          </ToolbarItem>
        </ToolbarContent>
      </Toolbar>

      {filtered.length === 0 ? (
        <EmptyState
          titleText="No persistent volume claims found"
          headingLevel="h2"
        >
          <EmptyStateBody>
            {pvcs.length > 0
              ? "No persistent volume claims match the current filter."
              : "There are no persistent volume claims available."}
          </EmptyStateBody>
        </EmptyState>
      ) : (
        <>
          <Table
            aria-label="Persistent Volume Claims"
            variant="compact"
            hasAnimations
          >
            <Thead>
              <Tr>
                <Th>Name</Th>
                <Th>Namespace</Th>
                <Th>Status</Th>
                <Th>Volume</Th>
                <Th>Capacity</Th>
                <Th>Storage Class</Th>
                <Th>Cluster</Th>
              </Tr>
            </Thead>
            <Tbody>
              {paginatedItems.map((pvc) => (
                <Tr key={pvc.id}>
                  <Td dataLabel="Name">
                    <span
                      style={{
                        fontWeight:
                          "var(--pf-t--global--font--weight--heading--default)",
                      }}
                    >
                      {pvc.name}
                    </span>
                  </Td>
                  <Td dataLabel="Namespace">
                    <span
                      style={{
                        fontSize: "var(--pf-t--global--font--size--sm)",
                        color: "var(--pf-t--global--text--color--subtle)",
                      }}
                    >
                      {pvc.namespace}
                    </span>
                  </Td>
                  <Td dataLabel="Status">
                    <Label color={pvcStatusColor(pvc.status)} isCompact>
                      {pvc.status}
                    </Label>
                  </Td>
                  <Td dataLabel="Volume">
                    <span
                      style={{
                        fontFamily: "var(--pf-t--global--font--family--mono)",
                        fontSize: "var(--pf-t--global--font--size--sm)",
                      }}
                    >
                      {pvc.pv_name ?? "\u2014"}
                    </span>
                  </Td>
                  <Td dataLabel="Capacity">
                    <Label color="blue" isCompact>
                      {parseCapacity(pvc.capacity).value}{" "}
                      {parseCapacity(pvc.capacity).unit}
                    </Label>
                  </Td>
                  <Td dataLabel="Storage Class">
                    <span
                      style={{
                        fontSize: "var(--pf-t--global--font--size--sm)",
                        color: "var(--pf-t--global--text--color--subtle)",
                      }}
                    >
                      {pvc.storage_class || "\u2014"}
                    </span>
                  </Td>
                  <Td dataLabel="Cluster">{pvc.cluster_id}</Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
          <Toolbar>
            <ToolbarContent>
              <ToolbarItem variant="pagination" align={{ default: "alignEnd" }}>
                <Pagination
                  itemCount={filtered.length}
                  perPage={PER_PAGE}
                  page={page}
                  onSetPage={(_event, p) => setPage(p)}
                  isCompact
                />
              </ToolbarItem>
            </ToolbarContent>
          </Toolbar>
        </>
      )}
    </>
  );
};

const StoragePage: React.FC = () => {
  const [activeTab, setActiveTab] = useState<string | number>(0);

  return (
    <div>
      <Flex
        alignItems={{ default: "alignItemsBaseline" }}
        gap={{ default: "gapSm" }}
        style={{ marginBottom: "var(--pf-t--global--spacer--lg)" }}
      >
        <FlexItem>
          <Title headingLevel="h1">Storage</Title>
        </FlexItem>
      </Flex>

      <Tabs
        activeKey={activeTab}
        onSelect={(_event, tabIndex) => setActiveTab(tabIndex)}
        aria-label="Storage tabs"
      >
        <Tab
          eventKey={0}
          title={<TabTitleText>Persistent Volumes</TabTitleText>}
        >
          <div style={{ paddingTop: "var(--pf-t--global--spacer--md)" }}>
            <PVTab />
          </div>
        </Tab>
        <Tab
          eventKey={1}
          title={<TabTitleText>Persistent Volume Claims</TabTitleText>}
        >
          <div style={{ paddingTop: "var(--pf-t--global--spacer--md)" }}>
            <PVCTab />
          </div>
        </Tab>
      </Tabs>
    </div>
  );
};

export default StoragePage;
