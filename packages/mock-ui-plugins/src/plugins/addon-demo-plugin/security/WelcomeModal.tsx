import {
  Button,
  Checkbox,
  Content,
  Icon,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  Split,
  SplitItem,
} from "@patternfly/react-core";
import { ShieldAltIcon } from "@patternfly/react-icons";
import { useCallback, useEffect, useState } from "react";

const STORAGE_KEY = "ome-addon-security-welcome-dismissed";

export default function WelcomeModal() {
  const [isOpen, setIsOpen] = useState(false);
  const [doNotShow, setDoNotShow] = useState(false);

  useEffect(() => {
    const dismissed = localStorage.getItem(STORAGE_KEY);
    if (!dismissed) {
      setIsOpen(true);
    }
  }, []);

  const handleClose = useCallback(() => {
    if (doNotShow) {
      localStorage.setItem(STORAGE_KEY, "true");
    }
    setIsOpen(false);
  }, [doNotShow]);

  if (!isOpen) return null;

  return (
    <Modal variant="medium" isOpen onClose={handleClose} aria-label="Welcome">
      <ModalHeader title="" />
      <ModalBody>
        <Split hasGutter>
          <SplitItem>
            <Icon size="2xl" className="pf-v6-u-color-200">
              <ShieldAltIcon />
            </Icon>
          </SplitItem>
          <SplitItem isFilled>
            <Content component="h2" className="pf-v6-u-mb-sm">
              Welcome to Security
            </Content>
            <Content component="p" className="pf-v6-u-mb-md">
              Scan images, enforce admission policies, and monitor compliance
              across your fleet. Detect vulnerabilities and misconfigurations
              before they reach production.
            </Content>
            <Content component="h3" className="pf-v6-u-mb-sm">
              What do you want to do next?
            </Content>
            <div className="pf-v6-u-display-flex pf-v6-u-flex-direction-column pf-v6-u-row-gap-sm">
              <Button variant="link" isInline onClick={handleClose}>
                Scan my first image
              </Button>
              <Button variant="link" isInline onClick={handleClose}>
                Configure admission policies
              </Button>
              <Button variant="link" isInline onClick={handleClose}>
                Explore the dashboard
              </Button>
            </div>
          </SplitItem>
        </Split>
      </ModalBody>
      <ModalFooter>
        <Checkbox
          id="security-do-not-show"
          label="Do not show this again"
          isChecked={doNotShow}
          onChange={(_e, checked) => setDoNotShow(checked)}
          className="pf-v6-u-mr-auto"
        />
        <Button variant="primary" onClick={handleClose}>
          Close
        </Button>
      </ModalFooter>
    </Modal>
  );
}
