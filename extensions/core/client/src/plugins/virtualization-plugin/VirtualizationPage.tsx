import { useExtensionInstall } from "@fleetshift/common";
import { Button, Content, PageSection, Title } from "@patternfly/react-core";
import { useCallback } from "react";

const VirtualizationPage = () => {
  const { uninstall } = useExtensionInstall();

  const handleUninstall = useCallback(() => {
    uninstall("virtualization-plugin");
  }, [uninstall]);

  return (
    <PageSection>
      <Title headingLevel="h1">Virtualization</Title>
      <Content component="p">
        Demonstration of virtual machine management with KubeVirt. VM operations
        are not implemented.
      </Content>
      <Button variant="secondary" isDanger onClick={handleUninstall}>
        Disable extension
      </Button>
    </PageSection>
  );
};

export default VirtualizationPage;
