import { useExtensionInstall } from "@fleetshift/common";
import { Button, Content, PageSection, Title } from "@patternfly/react-core";
import { useCallback } from "react";

const ObservabilityPage = () => {
  const { uninstall } = useExtensionInstall();

  const handleUninstall = useCallback(() => {
    uninstall("observability-plugin");
  }, [uninstall]);

  return (
    <PageSection>
      <Title headingLevel="h1">Observability</Title>
      <Content component="p">
        Unified metrics, logs, and traces across your fleet. Monitor cluster
        health, application performance, and resource utilization from a single
        pane of glass.
      </Content>
      <Button variant="secondary" isDanger onClick={handleUninstall}>
        Disable extension
      </Button>
    </PageSection>
  );
};

export default ObservabilityPage;
