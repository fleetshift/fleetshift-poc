import { useExtensionInstall } from "@fleetshift/common";
import { Button, Content, PageSection, Title } from "@patternfly/react-core";
import { useCallback } from "react";

const SecurityPage = () => {
  const { uninstall } = useExtensionInstall();

  const handleUninstall = useCallback(() => {
    uninstall("security-plugin");
  }, [uninstall]);

  return (
    <PageSection>
      <Title headingLevel="h1">Security</Title>
      <Content component="p">
        Scan images, enforce admission policies, and monitor compliance across
        your fleet. Detect vulnerabilities and misconfigurations before they
        reach production.
      </Content>
      <Button variant="secondary" isDanger onClick={handleUninstall}>
        Disable extension
      </Button>
    </PageSection>
  );
};

export default SecurityPage;
