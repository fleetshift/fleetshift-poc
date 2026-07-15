import "./SecurityPage.scss";

import {
  Label,
  Pagination,
  Toolbar,
  ToolbarContent,
  ToolbarItem,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import { useState } from "react";

import { CVE_DATA } from "./cveData";

const COLUMNS = [
  "CVE",
  "Images by severity",
  "Top CVSS",
  "Top NVD CVSS",
  "EPSS probability",
  "First discovered",
];

function SeverityBadges({
  critical,
  important,
  moderate,
  low,
}: {
  critical: number;
  important: number;
  moderate: number;
  low: number;
}) {
  return (
    <span className="ome-addon-security__severity-badges">
      {critical > 0 && (
        <Label color="red" isCompact>
          {critical} Critical
        </Label>
      )}
      {important > 0 && (
        <Label color="orange" isCompact>
          {important} Important
        </Label>
      )}
      {moderate > 0 && (
        <Label color="gold" isCompact>
          {moderate} Moderate
        </Label>
      )}
      {low > 0 && (
        <Label color="blue" isCompact>
          {low} Low
        </Label>
      )}
    </span>
  );
}

export default function CveTable() {
  const [page, setPage] = useState(1);
  const perPage = 20;

  return (
    <>
      <Toolbar>
        <ToolbarContent>
          <ToolbarItem>
            <Label color="blue" isCompact>
              {CVE_DATA.length} CVEs
            </Label>
          </ToolbarItem>
          <ToolbarItem variant="pagination" align={{ default: "alignEnd" }}>
            <Pagination
              itemCount={CVE_DATA.length}
              perPage={perPage}
              page={page}
              onSetPage={(_e, p) => setPage(p)}
              isCompact
            />
          </ToolbarItem>
        </ToolbarContent>
      </Toolbar>
      <Table aria-label="CVE table" variant="compact">
        <Thead>
          <Tr>
            {COLUMNS.map((col) => (
              <Th key={col}>{col}</Th>
            ))}
          </Tr>
        </Thead>
        <Tbody>
          {CVE_DATA.slice((page - 1) * perPage, page * perPage).map((row) => (
            <Tr key={row.id}>
              <Td dataLabel="CVE">
                <a
                  href={`https://www.cve.org/CVERecord?id=${row.cve}`}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  {row.cve}
                </a>
              </Td>
              <Td dataLabel="Images by severity">
                <SeverityBadges {...row.imagesBySeverity} />
              </Td>
              <Td dataLabel="Top CVSS">{row.topCvss.toFixed(1)}</Td>
              <Td dataLabel="Top NVD CVSS">{row.topNvdCvss.toFixed(1)}</Td>
              <Td dataLabel="EPSS probability">
                {(row.epssProbability * 100).toFixed(1)}%
              </Td>
              <Td dataLabel="First discovered">{row.firstDiscovered}</Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
    </>
  );
}
