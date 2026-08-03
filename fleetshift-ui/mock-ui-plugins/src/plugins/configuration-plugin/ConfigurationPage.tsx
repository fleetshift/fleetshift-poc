import { useExtensionInstall } from "@fleetshift/common";
import { Button, Content, PageSection, Title } from "@patternfly/react-core";
import { useCallback } from "react";

const ConfigurationPage = () => {
  const { uninstall } = useExtensionInstall();

  const handleUninstall = useCallback(() => {
    uninstall("configuration-plugin");
  }, [uninstall]);

  return (
    <PageSection>
      <Title headingLevel="h1">Configuration</Title>
      <Content component="p">
        Deploy and manage applications across your OpenShift fleet using GitOps
        and Helm. Keep workloads consistent as you scale from a single cluster
        to many.
      </Content>
      <Button variant="secondary" isDanger onClick={handleUninstall}>
        Disable extension
      </Button>
    </PageSection>
  );
};

export default ConfigurationPage;
