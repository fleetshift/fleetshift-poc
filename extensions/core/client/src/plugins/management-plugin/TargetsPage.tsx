import {
  Alert,
  Bullseye,
  Button,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  EmptyState,
  EmptyStateBody,
  Flex,
  FlexItem,
  Label,
  LabelGroup,
  SearchInput,
  Spinner,
  Title,
  Toolbar,
  ToolbarContent,
  ToolbarItem,
  Tooltip,
} from "@patternfly/react-core";
import { SyncAltIcon } from "@patternfly/react-icons";
import {
  ExpandableRowContent,
  Table,
  Tbody,
  Td,
  Th,
  Thead,
  Tr,
} from "@patternfly/react-table";
import { useCallback, useEffect, useMemo, useState } from "react";

import { listDeployments } from "./api";

interface TargetInfo {
  id: string;
  type: string;
  deployments: string[];
}

export default function TargetsPage() {
  const [targets, setTargets] = useState<TargetInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [nameFilter, setNameFilter] = useState("");

  const fetchTargets = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await listDeployments();
      const deployments = resp.deployments ?? [];

      const targetMap = new Map<string, string[]>();
      targetMap.set("kind-local", []);

      for (const dep of deployments) {
        for (const tid of dep.resolvedTargetIds ?? []) {
          const existing = targetMap.get(tid) ?? [];
          existing.push(dep.name);
          targetMap.set(tid, existing);
        }
      }

      setTargets(
        Array.from(targetMap.entries()).map(([id, deps]) => ({
          id,
          type: id.startsWith("kind") ? "kind" : "kubernetes",
          deployments: deps,
        })),
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch targets");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchTargets();
  }, [fetchTargets]);

  const filtered = useMemo(
    () =>
      targets.filter((t) =>
        nameFilter
          ? t.id.toLowerCase().includes(nameFilter.toLowerCase())
          : true,
      ),
    [targets, nameFilter],
  );

  const toggleExpand = useCallback((id: string) => {
    setExpandedId((prev) => (prev === id ? null : id));
  }, []);

  if (loading) {
    return (
      <Bullseye>
        <Spinner />
      </Bullseye>
    );
  }

  return (
    <div>
      <Flex
        alignItems={{ default: "alignItemsBaseline" }}
        gap={{ default: "gapSm" }}
        className="pf-v6-u-mb-lg"
      >
        <FlexItem>
          <Title headingLevel="h1">Targets</Title>
        </FlexItem>
        <FlexItem>
          <span className="pf-v6-u-font-size-sm pf-v6-u-text-color-subtle">
            {targets.length} registered
          </span>
        </FlexItem>
      </Flex>

      {error && (
        <Alert
          variant="warning"
          title={error}
          isInline
          className="pf-v6-u-mb-md"
        />
      )}

      {targets.length === 0 ? (
        <EmptyState titleText="No targets found" headingLevel="h2">
          <EmptyStateBody>
            Targets are registered automatically when the management plane
            starts.
          </EmptyStateBody>
          <Button variant="primary" onClick={fetchTargets}>
            Refresh
          </Button>
        </EmptyState>
      ) : (
        <>
          <Toolbar clearAllFilters={() => setNameFilter("")}>
            <ToolbarContent>
              <ToolbarItem>
                <SearchInput
                  placeholder="Filter by target ID"
                  value={nameFilter}
                  onChange={(_event, value) => setNameFilter(value)}
                  onClear={() => setNameFilter("")}
                />
              </ToolbarItem>
              <ToolbarItem>
                <Tooltip content="Refresh targets">
                  <Button
                    variant="plain"
                    aria-label="Refresh targets"
                    onClick={fetchTargets}
                  >
                    <SyncAltIcon />
                  </Button>
                </Tooltip>
              </ToolbarItem>
            </ToolbarContent>
          </Toolbar>

          {filtered.length === 0 ? (
            <EmptyState titleText="No matching targets" headingLevel="h2">
              <EmptyStateBody>
                No targets match the current filter. Try adjusting or clearing
                the filter.
              </EmptyStateBody>
              <Button variant="link" onClick={() => setNameFilter("")}>
                Clear filter
              </Button>
            </EmptyState>
          ) : (
            <Table
              aria-label="Registered targets"
              variant="compact"
              hasAnimations
            >
              <Thead>
                <Tr>
                  <Th screenReaderText="Row expansion" />
                  <Th>Target ID</Th>
                  <Th>Type</Th>
                  <Th>Deployments</Th>
                </Tr>
              </Thead>
              {filtered.map((t, rowIndex) => (
                <Tbody key={t.id} isExpanded={expandedId === t.id}>
                  <Tr>
                    <Td
                      expand={{
                        rowIndex,
                        isExpanded: expandedId === t.id,
                        onToggle: () => toggleExpand(t.id),
                      }}
                    />
                    <Td dataLabel="Target ID">
                      <span className="pf-v6-u-font-weight-bold">{t.id}</span>
                    </Td>
                    <Td dataLabel="Type">
                      <Label color="blue" isCompact>
                        {t.type}
                      </Label>
                    </Td>
                    <Td dataLabel="Deployments">
                      {t.deployments.length > 0 ? (
                        <LabelGroup>
                          {t.deployments.map((d) => (
                            <Label key={d} color="green" isCompact>
                              {d.replace("deployments/", "")}
                            </Label>
                          ))}
                        </LabelGroup>
                      ) : (
                        <span className="pf-v6-u-text-color-subtle">None</span>
                      )}
                    </Td>
                  </Tr>
                  <Tr isExpanded={expandedId === t.id}>
                    <Td />
                    <Td colSpan={3}>
                      <ExpandableRowContent>
                        <DescriptionList isHorizontal isCompact>
                          <DescriptionListGroup>
                            <DescriptionListTerm>Target ID</DescriptionListTerm>
                            <DescriptionListDescription>
                              <span className="pf-v6-u-font-family-monospace">
                                {t.id}
                              </span>
                            </DescriptionListDescription>
                          </DescriptionListGroup>
                          <DescriptionListGroup>
                            <DescriptionListTerm>
                              Accepted Resource Types
                            </DescriptionListTerm>
                            <DescriptionListDescription>
                              <Label color="grey" isCompact>
                                {t.id === "kind-local"
                                  ? "api.kind.cluster"
                                  : "kubernetes/*"}
                              </Label>
                            </DescriptionListDescription>
                          </DescriptionListGroup>
                          <DescriptionListGroup>
                            <DescriptionListTerm>
                              Active Deployments
                            </DescriptionListTerm>
                            <DescriptionListDescription>
                              {t.deployments.length}
                            </DescriptionListDescription>
                          </DescriptionListGroup>
                        </DescriptionList>
                      </ExpandableRowContent>
                    </Td>
                  </Tr>
                </Tbody>
              ))}
            </Table>
          )}
        </>
      )}
    </div>
  );
}
