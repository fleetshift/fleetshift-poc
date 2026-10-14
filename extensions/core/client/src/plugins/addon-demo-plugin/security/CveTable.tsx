import "./SecurityPage.scss";

import {
  Button,
  Content,
  Pagination,
  Split,
  SplitItem,
  Title,
} from "@patternfly/react-core";
import { DataView } from "@patternfly/react-data-view/dist/dynamic/DataView";
import { DataViewCheckboxFilter } from "@patternfly/react-data-view/dist/dynamic/DataViewCheckboxFilter";
import type { DataViewFilterOption } from "@patternfly/react-data-view/dist/dynamic/DataViewFilters";
import { DataViewFilters } from "@patternfly/react-data-view/dist/dynamic/DataViewFilters";
import type { DataViewTh } from "@patternfly/react-data-view/dist/dynamic/DataViewTable";
import { DataViewTable } from "@patternfly/react-data-view/dist/dynamic/DataViewTable";
import { DataViewTextFilter } from "@patternfly/react-data-view/dist/dynamic/DataViewTextFilter";
import { DataViewToolbar } from "@patternfly/react-data-view/dist/dynamic/DataViewToolbar";
import {
  useDataViewFilters,
  useDataViewPagination,
} from "@patternfly/react-data-view/dist/dynamic/Hooks";
import { ExternalLinkAltIcon } from "@patternfly/react-icons";
import { useMemo } from "react";

import {
  CVE_DATA,
  TOTAL_CVE_COUNT,
  TOTAL_DEPLOYMENT_COUNT,
  TOTAL_IMAGE_COUNT,
} from "./cveData";

interface CveFilters {
  name: string;
  severity: string[];
  status: string[];
}

const severityOptions: DataViewFilterOption[] = [
  { label: "Critical", value: "critical" },
  { label: "Important", value: "important" },
  { label: "Moderate", value: "moderate" },
  { label: "Low", value: "low" },
];

const statusOptions: DataViewFilterOption[] = [
  { label: "New", value: "new" },
  { label: "Deferred", value: "deferred" },
  { label: "False positive", value: "false-positive" },
];

const PER_PAGE_OPTIONS = [
  { title: "20", value: 20 },
  { title: "50", value: 50 },
  { title: "100", value: 100 },
];

function CriticalBadge({ count }: { count: number }) {
  return (
    <span className="ome-addon-security__critical-badge">
      <span className="ome-addon-security__critical-badge-count">{count}</span>
      <span className="pf-v6-u-text-color-subtle">Critical images</span>
    </span>
  );
}

const COLUMNS: DataViewTh[] = [
  "CVE",
  "Images by severity",
  "Top CVSS",
  "Top NVD CVSS",
  "EPSS probability",
  "First discovered",
];

export default function CveTable() {
  const { filters, onSetFilters, clearAllFilters } =
    useDataViewFilters<CveFilters>({
      initialFilters: { name: "", severity: [], status: [] },
    });

  const pagination = useDataViewPagination({ perPage: 20 });
  const { page, perPage } = pagination;

  const filteredData = useMemo(
    () =>
      CVE_DATA.filter(
        (row) =>
          (!filters.name ||
            row.cve.toLowerCase().includes(filters.name.toLowerCase())) &&
          (filters.severity.length === 0 ||
            filters.severity.includes(row.severity)) &&
          (filters.status.length === 0 || filters.status.includes(row.status)),
      ),
    [filters],
  );

  const rows = useMemo(
    () =>
      filteredData
        .slice((page - 1) * perPage, (page - 1) * perPage + perPage)
        .map((row) => [
          <a key={row.id} href="#">
            {row.cve}
          </a>,
          <CriticalBadge
            key={`sev-${row.id}`}
            count={row.imagesBySeverity.critical}
          />,
          `${row.topCvss.toFixed(1)} (${row.topCvssVersion})`,
          row.topNvdCvss !== null
            ? `${row.topNvdCvss.toFixed(1)} (${row.topNvdCvssVersion})`
            : "Not available",
          `${(row.epssProbability * 100).toFixed(3)}%`,
          row.firstDiscovered,
        ]),
    [filteredData, page, perPage],
  );

  return (
    <div className="ome-addon-security__cve-section">
      <Split className="pf-v6-u-mb-sm">
        <SplitItem isFilled>
          <Title headingLevel="h2" size="lg">
            Workload vulnerabilities
          </Title>
        </SplitItem>
        <SplitItem>
          <Button
            variant="link"
            isInline
            icon={<ExternalLinkAltIcon />}
            iconPosition="end"
          >
            Learn about vulnerability scoring
          </Button>
        </SplitItem>
      </Split>
      <Content
        component="p"
        className="pf-v6-u-mb-md pf-v6-u-text-color-subtle"
      >
        Prioritize CVEs affecting images and deployments across your fleet.
      </Content>

      <DataView>
        <DataViewToolbar
          clearAllFilters={clearAllFilters}
          filters={
            <DataViewFilters
              onChange={(_e, values) => onSetFilters(values)}
              values={filters}
            >
              <DataViewTextFilter
                filterId="name"
                title="CVE"
                placeholder="Filter by CVE name"
              />
              <DataViewCheckboxFilter
                filterId="severity"
                title="CVE severity"
                placeholder="Filter by severity"
                options={severityOptions}
              />
              <DataViewCheckboxFilter
                filterId="status"
                title="CVE status"
                placeholder="Filter by status"
                options={statusOptions}
              />
            </DataViewFilters>
          }
          pagination={
            <Pagination
              isCompact
              perPageOptions={PER_PAGE_OPTIONS}
              itemCount={filteredData.length}
              {...pagination}
            />
          }
        />
        <div className="ome-addon-security__tab-pills pf-v6-u-mb-md">
          <Split>
            <SplitItem className="pf-v6-u-mr-sm pf-v6-u-font-size-sm pf-v6-u-font-weight-bold">
              {TOTAL_CVE_COUNT.toLocaleString()} CVEs
            </SplitItem>
            <SplitItem className="pf-v6-u-mr-sm pf-v6-u-font-size-sm pf-v6-u-text-color-subtle">
              {TOTAL_IMAGE_COUNT.toLocaleString()} Images
            </SplitItem>
            <SplitItem className="pf-v6-u-font-size-sm pf-v6-u-text-color-subtle">
              {TOTAL_DEPLOYMENT_COUNT.toLocaleString()} Deployments
            </SplitItem>
          </Split>
        </div>
        <DataViewTable aria-label="CVE table" columns={COLUMNS} rows={rows} />
        <DataViewToolbar
          pagination={
            <Pagination
              isCompact
              perPageOptions={PER_PAGE_OPTIONS}
              itemCount={filteredData.length}
              {...pagination}
            />
          }
        />
      </DataView>
    </div>
  );
}
