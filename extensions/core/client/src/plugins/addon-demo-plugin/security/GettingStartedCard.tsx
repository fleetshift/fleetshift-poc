import {
  Alert,
  Card,
  CardBody,
  CardHeader,
  Divider,
  Dropdown,
  DropdownItem,
  DropdownList,
  Flex,
  FlexItem,
  MenuToggle,
  Title,
} from "@patternfly/react-core";
import {
  AngleDownIcon,
  AngleRightIcon,
  ArrowRightIcon,
  EllipsisVIcon,
  ExternalLinkAltIcon,
} from "@patternfly/react-icons";
import clsx from "clsx";
import type { ReactNode } from "react";
import { Fragment, useState } from "react";

type ColumnColor = "green" | "purple" | "warning";

interface GsLink {
  text: string;
  external?: boolean;
  suffix?: string;
}

interface GsColumnConfig {
  title: string;
  color: ColumnColor;
  description: string;
  links: GsLink[];
  footerText: string;
}

const COLUMNS: GsColumnConfig[] = [
  {
    title: "Quick starts",
    color: "green",
    description:
      "Get started with security features using step-by-step documentation.",
    links: [
      { text: "Review workload vulnerabilities across the fleet" },
      { text: "Configure vulnerability reporting for critical images" },
    ],
    footerText: "View all quick starts",
  },
  {
    title: "Feature highlights",
    color: "purple",
    description:
      "Read about the latest Advanced Cluster Security capabilities and security workflows.",
    links: [
      {
        text: "Prioritize CVEs with EPSS and CVSS context",
        external: true,
        suffix: "6 min read",
      },
      {
        text: "Fleet-wide vulnerability dashboards in OME",
        external: true,
        suffix: "4 min read",
      },
    ],
    footerText: "Visit the blog",
  },
  {
    title: "Related operators",
    color: "warning",
    description:
      "Extend security coverage with operators that integrate into your fleet management workflow.",
    links: [
      { text: "Advanced Cluster Security Operator" },
      { text: "Compliance Operator" },
      { text: "Red Hat Quay vulnerability scanning" },
    ],
    footerText: "Learn more about Operators",
  },
];

function GsColumnHeader({
  title,
  color,
  compact,
}: {
  title: string;
  color: ColumnColor;
  compact?: boolean;
}) {
  return (
    <Alert
      variant="info"
      isInline
      isPlain
      title={title}
      className={clsx(
        "ome-addon-security__gs-header-label",
        `ome-addon-security__gs-header-label--${color}`,
        compact && "ome-addon-security__gs-header-label--compact",
      )}
    />
  );
}

function GsColumn({
  config,
  children,
}: {
  config: GsColumnConfig;
  children: ReactNode;
}) {
  return (
    <div className="ome-addon-security__gs-column">
      <GsColumnHeader title={config.title} color={config.color} />
      <div className="ome-addon-security__gs-column-body">{children}</div>
    </div>
  );
}

function GsColumnContent({ config }: { config: GsColumnConfig }) {
  return (
    <>
      <p className="pf-v6-u-mb-md pf-v6-u-text-color-subtle pf-v6-u-font-size-sm">
        {config.description}
      </p>
      <ul className="ome-addon-security__gs-links">
        {config.links.map((link) => (
          <li key={link.text}>
            <a
              href="#"
              {...(link.external
                ? { target: "_blank", rel: "noopener noreferrer" }
                : {})}
            >
              {link.text}
              {link.suffix && (
                <>
                  {" "}
                  <span className="pf-v6-u-text-color-subtle">
                    &middot; {link.suffix}
                  </span>
                </>
              )}
              {link.external && (
                <>
                  {" "}
                  <ExternalLinkAltIcon className="pf-v6-u-font-size-xs" />
                </>
              )}
            </a>
          </li>
        ))}
      </ul>
      <a href="#" className="ome-addon-security__gs-footer-link">
        {config.footerText} <ArrowRightIcon />
      </a>
    </>
  );
}

export default function GettingStartedCard() {
  const [isExpanded, setIsExpanded] = useState(true);
  const [isKebabOpen, setIsKebabOpen] = useState(false);

  return (
    <Card
      variant="secondary"
      className="ome-addon-security__getting-started pf-v6-u-mb-lg"
    >
      <CardHeader
        actions={{
          actions: (
            <Dropdown
              isOpen={isKebabOpen}
              onSelect={() => setIsKebabOpen(false)}
              onOpenChange={setIsKebabOpen}
              toggle={(toggleRef) => (
                <MenuToggle
                  ref={toggleRef}
                  variant="plain"
                  onClick={() => setIsKebabOpen(!isKebabOpen)}
                  aria-label="Card actions"
                >
                  <EllipsisVIcon />
                </MenuToggle>
              )}
              popperProps={{ position: "right" }}
            >
              <DropdownList>
                <DropdownItem key="hide" onClick={() => setIsExpanded(false)}>
                  Hide getting started
                </DropdownItem>
              </DropdownList>
            </Dropdown>
          ),
          hasNoOffset: true,
        }}
      >
        <Flex
          alignItems={{ default: "alignItemsCenter" }}
          gap={{ default: "gapMd" }}
          className="ome-addon-security__getting-started-header"
          onClick={() => setIsExpanded(!isExpanded)}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === " ") {
              e.preventDefault();
              setIsExpanded(!isExpanded);
            }
          }}
          role="button"
          tabIndex={0}
          aria-expanded={isExpanded}
          style={{ cursor: "pointer" }}
        >
          <FlexItem>
            {isExpanded ? <AngleDownIcon /> : <AngleRightIcon />}
          </FlexItem>
          <FlexItem>
            <Title headingLevel="h2" size="md">
              Getting started
            </Title>
          </FlexItem>
          {!isExpanded &&
            COLUMNS.map((col) => (
              <FlexItem key={col.title}>
                <GsColumnHeader title={col.title} color={col.color} compact />
              </FlexItem>
            ))}
        </Flex>
      </CardHeader>
      {isExpanded && (
        <CardBody>
          <Flex
            gap={{ default: "gapNone" }}
            alignItems={{ default: "alignItemsStretch" }}
          >
            {COLUMNS.map((col, i) => (
              <Fragment key={col.title}>
                {i > 0 && (
                  <Divider
                    orientation={{ default: "vertical" }}
                    className="pf-v6-u-mx-md"
                  />
                )}
                <FlexItem flex={{ default: "flex_1" }}>
                  <GsColumn config={col}>
                    <GsColumnContent config={col} />
                  </GsColumn>
                </FlexItem>
              </Fragment>
            ))}
          </Flex>
        </CardBody>
      )}
    </Card>
  );
}
