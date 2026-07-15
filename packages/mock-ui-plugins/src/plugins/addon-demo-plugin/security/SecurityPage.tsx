import {
  Content,
  PageSection,
  Tab,
  Tabs,
  TabTitleText,
  Title,
} from "@patternfly/react-core";
import { useState } from "react";

import CveTable from "./CveTable";
import DeploymentsTab from "./DeploymentsTab";
import GettingStartedCard from "./GettingStartedCard";
import ImagesTab from "./ImagesTab";
import WelcomeModal from "./WelcomeModal";

type TabKey = "overview" | "images" | "deployments";

const SecurityPage = () => {
  const [activeTab, setActiveTab] = useState<TabKey>("overview");

  return (
    <PageSection>
      <WelcomeModal />
      <Title headingLevel="h1" className="pf-v6-u-mb-md">
        Security
      </Title>
      <Content component="p" className="pf-v6-u-mb-lg">
        Scan images, enforce admission policies, and monitor compliance across
        your fleet.
      </Content>
      <Tabs
        activeKey={activeTab}
        onSelect={(_e, key) => setActiveTab(key as TabKey)}
      >
        <Tab
          eventKey="overview"
          title={<TabTitleText>Overview</TabTitleText>}
          aria-label="Overview"
        >
          <div className="pf-v6-u-mt-lg">
            <GettingStartedCard />
            <CveTable />
          </div>
        </Tab>
        <Tab
          eventKey="images"
          title={<TabTitleText>Images</TabTitleText>}
          aria-label="Images"
        >
          <div className="pf-v6-u-mt-lg">
            <ImagesTab />
          </div>
        </Tab>
        <Tab
          eventKey="deployments"
          title={<TabTitleText>Deployments</TabTitleText>}
          aria-label="Deployments"
        >
          <div className="pf-v6-u-mt-lg">
            <DeploymentsTab />
          </div>
        </Tab>
      </Tabs>
    </PageSection>
  );
};

export default SecurityPage;
