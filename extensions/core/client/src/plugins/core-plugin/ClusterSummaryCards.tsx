import {
  Card,
  CardBody,
  Content,
  Grid,
  GridItem,
  Title,
} from "@patternfly/react-core";

import {
  type ClusterRow,
  isFailureState,
  isHealthyState,
} from "./clusterTypes";

interface SummaryCard {
  label: string;
  value: number;
  color?: string;
}

function buildSummary(rows: ClusterRow[]): SummaryCard[] {
  const healthy = rows.filter((r) =>
    isHealthyState(r.result.resource.state),
  ).length;
  const needsAttention = rows.filter((r) =>
    isFailureState(r.result.resource.state),
  ).length;

  return [
    { label: "Total Clusters", value: rows.length },
    {
      label: "Healthy",
      value: healthy,
      color: "var(--pf-t--global--color--status--success--default)",
    },
    {
      label: "Needs Attention",
      value: needsAttention,
      color:
        needsAttention > 0
          ? "var(--pf-t--global--color--status--danger--default)"
          : undefined,
    },
  ];
}

export default function ClusterSummaryCards({ rows }: { rows: ClusterRow[] }) {
  const cards = buildSummary(rows);

  return (
    <Grid hasGutter>
      {cards.map((card) => (
        <GridItem key={card.label} span={4}>
          <Card isPlain isCompact>
            <CardBody>
              <Content component="p">{card.label}</Content>
              <Title headingLevel="h2" size="2xl" style={{ color: card.color }}>
                {card.value}
              </Title>
            </CardBody>
          </Card>
        </GridItem>
      ))}
    </Grid>
  );
}
