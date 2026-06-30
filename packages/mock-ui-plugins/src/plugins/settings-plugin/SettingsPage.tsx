import { Content, Title } from "@patternfly/react-core";

import NavLayoutEditor from "./NavLayoutEditor";

const SettingsPage = () => (
  <>
    <Title headingLevel="h1">Settings</Title>
    <Content component="p" className="pf-v6-u-mb-lg">
      Manage your workspace preferences.
    </Content>
    <NavLayoutEditor />
  </>
);

export default SettingsPage;
