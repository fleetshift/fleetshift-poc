import { useState, useMemo } from "react";
import {
  Alert,
  Bullseye,
  Card,
  CardBody,
  CardTitle,
  DescriptionList,
  DescriptionListGroup,
  DescriptionListTerm,
  DescriptionListDescription,
  EmptyState,
  EmptyStateBody,
  Flex,
  FlexItem,
  Grid,
  GridItem,
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
import {
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  ExpandableRowContent,
} from "@patternfly/react-table";
import { Link } from "react-router-dom";
import { useScalprum } from "@scalprum/react-core";
import { useClowderStore } from "./clowderStore";

interface FleetShiftApi {
  fleetshift: {
    getClusterName: (clusterId: string) => string;
  };
}

const PER_PAGE = 20;

function readyLabel(ready: number, total: number) {
  const allReady = ready === total && total > 0;
  return (
    <Label color={allReady ? "green" : "orange"} isCompact>
      {ready}/{total} ready
    </Label>
  );
}

// --- ClowdApps Tab ---

const ClowdAppsTab: React.FC<{ clusterIds: string[] }> = ({ clusterIds }) => {
  const { api } = useScalprum<{ api: FleetShiftApi }>();
  const { apps, deployMap, loading, error } = useClowderStore();
  const [nameFilter, setNameFilter] = useState("");
  const [page, setPage] = useState(1);
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

  const toggleRow = (key: string) => {
    setExpandedRows((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const filtered = useMemo(
    () =>
      apps.filter((a) =>
        nameFilter
          ? a.name.toLowerCase().includes(nameFilter.toLowerCase())
          : true,
      ),
    [apps, nameFilter],
  );

  const paginated = useMemo(
    () => filtered.slice((page - 1) * PER_PAGE, page * PER_PAGE),
    [filtered, page],
  );

  if (loading) {
    return (
      <Bullseye style={{ padding: "var(--pf-t--global--spacer--2xl) 0" }}>
        <Spinner />
      </Bullseye>
    );
  }

  return (
    <div style={{ paddingTop: "var(--pf-t--global--spacer--md)" }}>
      {error && (
        <Alert
          variant="warning"
          isInline
          title={error}
          style={{ marginBottom: "var(--pf-t--global--spacer--md)" }}
        />
      )}
      <Toolbar>
        <ToolbarContent>
          <ToolbarItem>
            <SearchInput
              placeholder="Filter by name"
              value={nameFilter}
              onChange={(_event, value) => setNameFilter(value)}
              onClear={() => setNameFilter("")}
            />
          </ToolbarItem>
          <ToolbarItem variant="pagination" align={{ default: "alignEnd" }}>
            <Pagination
              itemCount={filtered.length}
              perPage={PER_PAGE}
              page={page}
              onSetPage={(_e, p) => setPage(p)}
              isCompact
            />
          </ToolbarItem>
        </ToolbarContent>
      </Toolbar>

      {filtered.length === 0 ? (
        <EmptyState titleText="No ClowdApps found" headingLevel="h2">
          <EmptyStateBody>
            {apps.length > 0
              ? "No ClowdApps match the current filter."
              : "No ClowdApps are deployed on this cluster."}
          </EmptyStateBody>
        </EmptyState>
      ) : (
        <Table aria-label="ClowdApps" variant="compact" hasAnimations>
          <Thead>
            <Tr>
              <Th screenReaderText="Expand" />
              <Th>Name</Th>
              {clusterIds.length > 1 && <Th>Cluster</Th>}
              <Th>Namespace</Th>
              <Th>Environment</Th>
              <Th>Deployments</Th>
              <Th>Status</Th>
            </Tr>
          </Thead>
          {paginated.map((app, rowIndex) => {
            const rowKey = `${app.cluster_id}-${app.namespace}-${app.name}`;
            const isExpanded = expandedRows.has(rowKey);
            return (
              <Tbody key={rowKey} isExpanded={isExpanded}>
                <Tr>
                  <Td
                    expand={{
                      rowIndex,
                      isExpanded,
                      onToggle: () => toggleRow(rowKey),
                    }}
                  />
                  <Td dataLabel="Name">
                    <span
                      style={{
                        fontWeight:
                          "var(--pf-t--global--font--weight--heading--default)",
                      }}
                    >
                      {app.name}
                    </span>
                  </Td>
                  {clusterIds.length > 1 && (
                    <Td dataLabel="Cluster">
                      <Label color="grey" isCompact>
                        {api.fleetshift.getClusterName(app.cluster_id)}
                      </Label>
                    </Td>
                  )}
                  <Td dataLabel="Namespace">{app.namespace}</Td>
                  <Td dataLabel="Environment">
                    <Label color="blue" isCompact>
                      {app.envName}
                    </Label>
                  </Td>
                  <Td dataLabel="Deployments">{app.deploymentCount}</Td>
                  <Td dataLabel="Status">
                    {readyLabel(app.readyDeployments, app.managedDeployments)}
                  </Td>
                </Tr>
                <Tr isExpanded={isExpanded}>
                  <Td colSpan={clusterIds.length > 1 ? 7 : 6}>
                    <ExpandableRowContent>
                      <Grid hasGutter>
                        <GridItem md={6}>
                          <Card isPlain isCompact>
                            <CardTitle>Deployments</CardTitle>
                            <CardBody>
                              {app.deployments.length === 0 ? (
                                <span>None</span>
                              ) : (
                                <Table
                                  variant="compact"
                                  aria-label="Deployments"
                                  hasAnimations
                                >
                                  <Thead>
                                    <Tr>
                                      <Th>Name</Th>
                                      <Th>Image</Th>
                                      <Th>Public</Th>
                                    </Tr>
                                  </Thead>
                                  <Tbody>
                                    {app.deployments.map((d) => {
                                      const k8sName = `${app.name}-${d.name}`;
                                      const depId =
                                        deployMap[
                                          `${app.cluster_id}/${k8sName}`
                                        ];
                                      return (
                                        <Tr key={d.name}>
                                          <Td>
                                            {depId ? (
                                              <Link
                                                to={`/deployments/${depId}`}
                                              >
                                                {d.name}
                                              </Link>
                                            ) : (
                                              d.name
                                            )}
                                          </Td>
                                          <Td>
                                            <code
                                              style={{
                                                fontSize:
                                                  "var(--pf-t--global--font--size--xs)",
                                                wordBreak: "break-all",
                                              }}
                                            >
                                              {d.image}
                                            </code>
                                          </Td>
                                          <Td>
                                            {d.public ? (
                                              <Label color="green" isCompact>
                                                Yes
                                              </Label>
                                            ) : (
                                              <Label color="grey" isCompact>
                                                No
                                              </Label>
                                            )}
                                          </Td>
                                        </Tr>
                                      );
                                    })}
                                  </Tbody>
                                </Table>
                              )}
                            </CardBody>
                          </Card>
                        </GridItem>
                        <GridItem md={6}>
                          <Card isPlain isCompact>
                            <CardTitle>Resources</CardTitle>
                            <CardBody>
                              <DescriptionList isCompact isHorizontal>
                                {app.dependencies.length > 0 && (
                                  <DescriptionListGroup>
                                    <DescriptionListTerm>
                                      Dependencies
                                    </DescriptionListTerm>
                                    <DescriptionListDescription>
                                      <LabelGroup>
                                        {app.dependencies.map((d) => (
                                          <Label key={d} color="blue" isCompact>
                                            {d}
                                          </Label>
                                        ))}
                                      </LabelGroup>
                                    </DescriptionListDescription>
                                  </DescriptionListGroup>
                                )}
                                {app.optionalDependencies.length > 0 && (
                                  <DescriptionListGroup>
                                    <DescriptionListTerm>
                                      Optional Deps
                                    </DescriptionListTerm>
                                    <DescriptionListDescription>
                                      <LabelGroup>
                                        {app.optionalDependencies.map((d) => (
                                          <Label key={d} color="grey" isCompact>
                                            {d}
                                          </Label>
                                        ))}
                                      </LabelGroup>
                                    </DescriptionListDescription>
                                  </DescriptionListGroup>
                                )}
                                {app.database && (
                                  <DescriptionListGroup>
                                    <DescriptionListTerm>
                                      Database
                                    </DescriptionListTerm>
                                    <DescriptionListDescription>
                                      {app.database.name ?? "yes"}
                                      {app.database.version
                                        ? ` (v${app.database.version})`
                                        : ""}
                                    </DescriptionListDescription>
                                  </DescriptionListGroup>
                                )}
                                {app.inMemoryDb && (
                                  <DescriptionListGroup>
                                    <DescriptionListTerm>
                                      In-Memory DB
                                    </DescriptionListTerm>
                                    <DescriptionListDescription>
                                      <Label color="blue" isCompact>
                                        Enabled
                                      </Label>
                                    </DescriptionListDescription>
                                  </DescriptionListGroup>
                                )}
                                {app.kafkaTopics.length > 0 && (
                                  <DescriptionListGroup>
                                    <DescriptionListTerm>
                                      Kafka Topics
                                    </DescriptionListTerm>
                                    <DescriptionListDescription>
                                      <LabelGroup>
                                        {app.kafkaTopics.map((t) => (
                                          <Label
                                            key={t.topicName}
                                            color="purple"
                                            isCompact
                                          >
                                            {t.topicName}
                                          </Label>
                                        ))}
                                      </LabelGroup>
                                    </DescriptionListDescription>
                                  </DescriptionListGroup>
                                )}
                                {app.featureFlags && (
                                  <DescriptionListGroup>
                                    <DescriptionListTerm>
                                      Feature Flags
                                    </DescriptionListTerm>
                                    <DescriptionListDescription>
                                      <Label color="blue" isCompact>
                                        Enabled
                                      </Label>
                                    </DescriptionListDescription>
                                  </DescriptionListGroup>
                                )}
                                {app.jobs.length > 0 && (
                                  <DescriptionListGroup>
                                    <DescriptionListTerm>
                                      Jobs
                                    </DescriptionListTerm>
                                    <DescriptionListDescription>
                                      <LabelGroup>
                                        {app.jobs.map((j) => (
                                          <Label key={j} color="grey" isCompact>
                                            {j}
                                          </Label>
                                        ))}
                                      </LabelGroup>
                                    </DescriptionListDescription>
                                  </DescriptionListGroup>
                                )}
                              </DescriptionList>
                            </CardBody>
                          </Card>
                        </GridItem>
                      </Grid>
                    </ExpandableRowContent>
                  </Td>
                </Tr>
              </Tbody>
            );
          })}
        </Table>
      )}
    </div>
  );
};

// --- ClowdEnvironments Tab ---

const ClowdEnvsTab: React.FC<{ clusterIds: string[] }> = () => {
  const { environments: envs, loading, error } = useClowderStore();

  if (loading) {
    return (
      <Bullseye style={{ padding: "var(--pf-t--global--spacer--2xl) 0" }}>
        <Spinner />
      </Bullseye>
    );
  }

  if (error && envs.length === 0) {
    return (
      <Alert
        variant="danger"
        isInline
        title="Failed to load ClowdEnvironments"
        style={{ margin: "var(--pf-t--global--spacer--md) 0" }}
      >
        {error}
      </Alert>
    );
  }

  if (envs.length === 0) {
    return (
      <EmptyState titleText="No ClowdEnvironments found" headingLevel="h2">
        <EmptyStateBody>
          No ClowdEnvironments are deployed on this cluster.
        </EmptyStateBody>
      </EmptyState>
    );
  }

  return (
    <Grid hasGutter style={{ paddingTop: "var(--pf-t--global--spacer--md)" }}>
      {envs.map((env) => (
        <GridItem key={`${env.cluster_id}-${env.name}`} md={6}>
          <Card isFullHeight>
            <CardTitle>
              <Flex
                alignItems={{ default: "alignItemsCenter" }}
                gap={{ default: "gapSm" }}
              >
                <FlexItem>
                  <strong>{env.name}</strong>
                </FlexItem>
                <FlexItem>
                  {readyLabel(env.readyDeployments, env.managedDeployments)}
                </FlexItem>
              </Flex>
            </CardTitle>
            <CardBody>
              <DescriptionList isCompact isHorizontal>
                <DescriptionListGroup>
                  <DescriptionListTerm>Namespace</DescriptionListTerm>
                  <DescriptionListDescription>
                    {env.targetNamespace || env.namespace}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>Apps</DescriptionListTerm>
                  <DescriptionListDescription>
                    <Label color="blue" isCompact>
                      {env.appCount}
                    </Label>
                  </DescriptionListDescription>
                </DescriptionListGroup>
                {Object.keys(env.providers).length > 0 && (
                  <DescriptionListGroup>
                    <DescriptionListTerm>Providers</DescriptionListTerm>
                    <DescriptionListDescription>
                      <LabelGroup>
                        {Object.entries(env.providers).map(([key, val]) => (
                          <Label key={key} color="purple" isCompact>
                            {key}: {val.mode ?? "default"}
                          </Label>
                        ))}
                      </LabelGroup>
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                )}
              </DescriptionList>

              {env.apps.length > 0 && (
                <div style={{ marginTop: "var(--pf-t--global--spacer--md)" }}>
                  <Title headingLevel="h4" size="md">
                    Apps in Environment
                  </Title>
                  <Table
                    variant="compact"
                    aria-label="Environment apps"
                    hasAnimations
                  >
                    <Thead>
                      <Tr>
                        <Th>App</Th>
                        <Th>Status</Th>
                      </Tr>
                    </Thead>
                    <Tbody>
                      {env.apps.map((a) => (
                        <Tr key={a.name}>
                          <Td>{a.name}</Td>
                          <Td>
                            {readyLabel(
                              a.readyDeployments,
                              a.managedDeployments,
                            )}
                          </Td>
                        </Tr>
                      ))}
                    </Tbody>
                  </Table>
                </div>
              )}
            </CardBody>
          </Card>
        </GridItem>
      ))}
    </Grid>
  );
};

// --- Main Page ---

const ClowderPage: React.FC<{ clusterIds: string[] }> = ({ clusterIds }) => {
  const [activeTab, setActiveTab] = useState<string | number>(0);

  return (
    <div>
      <Flex
        alignItems={{ default: "alignItemsBaseline" }}
        gap={{ default: "gapSm" }}
        style={{ marginBottom: "var(--pf-t--global--spacer--lg)" }}
      >
        <FlexItem>
          <Title headingLevel="h1">Clowder</Title>
        </FlexItem>
        <FlexItem>
          <span
            style={{
              fontSize: "var(--pf-t--global--font--size--sm)",
              color: "var(--pf-t--global--text--color--subtle)",
            }}
          >
            ClowdApps & Environments
          </span>
        </FlexItem>
      </Flex>

      <Tabs
        activeKey={activeTab}
        onSelect={(_event, tabIndex) => setActiveTab(tabIndex)}
        aria-label="Clowder tabs"
      >
        <Tab eventKey={0} title={<TabTitleText>ClowdApps</TabTitleText>}>
          <ClowdAppsTab clusterIds={clusterIds} />
        </Tab>
        <Tab eventKey={1} title={<TabTitleText>Environments</TabTitleText>}>
          <ClowdEnvsTab clusterIds={clusterIds} />
        </Tab>
      </Tabs>
    </div>
  );
};

export default ClowderPage;
