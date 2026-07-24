import {
  Bullseye,
  Button,
  Checkbox,
  Content,
  Flex,
  FlexItem,
  Modal,
  ModalBody,
  ModalHeader,
  Split,
  SplitItem,
} from "@patternfly/react-core";
import { ExternalLinkAltIcon, ShieldAltIcon } from "@patternfly/react-icons";
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
        <Split hasGutter className="ome-addon-security__welcome">
          <SplitItem>
            <Bullseye>
              <div className="ome-addon-security__welcome-icon">
                <ShieldAltIcon className="ome-addon-security__welcome-shield" />
              </div>
            </Bullseye>
          </SplitItem>
          <SplitItem isFilled>
            <Content component="h2" className="pf-v6-u-mb-sm">
              Welcome to Security in OME
            </Content>
            <Content
              component="p"
              className="pf-v6-u-mb-lg pf-v6-u-text-color-subtle"
            >
              Monitor workload vulnerabilities, compliance posture, and risk
              across your fleet. Detect issues early and enforce policies before
              they reach production.
            </Content>
            <Content component="h3" className="pf-v6-u-mb-md">
              What do you want to do next?
            </Content>
            <Flex className="pf-v6-u-mb-lg" gap={{ default: "gapMd" }}>
              <FlexItem>
                <Button variant="primary" onClick={handleClose}>
                  View workload vulnerabilities
                </Button>
              </FlexItem>
              <FlexItem>
                <Button variant="secondary" onClick={handleClose}>
                  Start tour
                </Button>
              </FlexItem>
              <FlexItem>
                <Button
                  variant="link"
                  isInline
                  icon={<ExternalLinkAltIcon />}
                  iconPosition="end"
                  onClick={handleClose}
                >
                  Resources
                </Button>
              </FlexItem>
            </Flex>
            <Checkbox
              id="security-do-not-show"
              label="Do not show this again"
              isChecked={doNotShow}
              onChange={(_e, checked) => setDoNotShow(checked)}
            />
          </SplitItem>
        </Split>
      </ModalBody>
    </Modal>
  );
}
