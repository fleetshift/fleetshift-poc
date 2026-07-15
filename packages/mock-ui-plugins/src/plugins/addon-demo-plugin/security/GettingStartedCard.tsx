import {
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  ExpandableSection,
  Grid,
  GridItem,
  List,
  ListItem,
  Title,
} from "@patternfly/react-core";
import { useState } from "react";

export default function GettingStartedCard() {
  const [isExpanded, setIsExpanded] = useState(true);

  return (
    <Card className="pf-v6-u-mb-lg">
      <CardHeader>
        <CardTitle>
          <ExpandableSection
            toggleText={isExpanded ? "Getting started" : "Getting started"}
            isExpanded={isExpanded}
            onToggle={(_e, expanded) => setIsExpanded(expanded)}
            isIndented
          >
            <Grid hasGutter>
              <GridItem span={4}>
                <Title headingLevel="h3" size="md" className="pf-v6-u-mb-sm">
                  Quick starts
                </Title>
                <List isPlain>
                  <ListItem>
                    <a href="#">Integrate with your CI/CD pipeline</a>
                  </ListItem>
                  <ListItem>
                    <a href="#">Configure image scanning policies</a>
                  </ListItem>
                  <ListItem>
                    <a href="#">Set up vulnerability alerting</a>
                  </ListItem>
                </List>
              </GridItem>
              <GridItem span={4}>
                <Title headingLevel="h3" size="md" className="pf-v6-u-mb-sm">
                  Feature highlights
                </Title>
                <List isPlain>
                  <ListItem>
                    <a href="#">Image vulnerability scanning</a>
                  </ListItem>
                  <ListItem>
                    <a href="#">Network policy visualization</a>
                  </ListItem>
                  <ListItem>
                    <a href="#">Compliance operator dashboards</a>
                  </ListItem>
                </List>
              </GridItem>
              <GridItem span={4}>
                <Title headingLevel="h3" size="md" className="pf-v6-u-mb-sm">
                  Related resources
                </Title>
                <List isPlain>
                  <ListItem>
                    <a href="#">ACS documentation</a>
                  </ListItem>
                  <ListItem>
                    <a href="#">Compliance operator guide</a>
                  </ListItem>
                  <ListItem>
                    <a href="#">Security best practices</a>
                  </ListItem>
                </List>
              </GridItem>
            </Grid>
          </ExpandableSection>
        </CardTitle>
      </CardHeader>
      <CardBody />
    </Card>
  );
}
