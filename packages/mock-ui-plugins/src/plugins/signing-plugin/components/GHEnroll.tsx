import {
  Alert,
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Split,
  SplitItem,
  Stack,
  StackItem,
} from "@patternfly/react-core";
import { ExternalLinkAltIcon } from "@patternfly/react-icons";

import ghSigningKeyImg from "../assets/gh-signing-screen.png";
import ghSigningKeyDarkImg from "../assets/gh-signing-screen-dark.png";
import MotionPollingAnimation from "./MotionPollingAnimation";
import { ReactNode } from "react";

export type GHEnrollProps = {
  githubUsername?: string | null;
  setGhPollEnabled: (enabled: boolean) => void;
  ghPollEnabled?: boolean;
  ghKeyError?: ReactNode;
};

const GHEnroll = ({
  githubUsername,
  setGhPollEnabled,
  ghPollEnabled,
  ghKeyError,
}: GHEnrollProps) => {
  return (
    <Card isCompact className="pf-v6-u-mt-lg">
      <CardHeader>
        <CardTitle>Register on GitHub</CardTitle>
      </CardHeader>
      <CardBody>
        <Stack>
          <StackItem>
            <p>
              Copy your public key above and add it as a{" "}
              <strong>Signing Key</strong> on GitHub.
            </p>
          </StackItem>
          <StackItem>
            <img
              src={ghSigningKeyImg}
              alt="GitHub SSH key settings page — select Signing Key from the key type dropdown"
              className="fs-setup__screenshot fs-setup__screenshot--light"
            />
            <img
              src={ghSigningKeyDarkImg}
              alt="GitHub SSH key settings page — select Signing Key from the key type dropdown"
              className="fs-setup__screenshot fs-setup__screenshot--dark"
            />
          </StackItem>
          <StackItem>
            {ghPollEnabled ? (
              <MotionPollingAnimation>
                Waiting for key to appear on GitHub&hellip;
              </MotionPollingAnimation>
            ) : (
              <Split hasGutter className="pf-v6-u-mt-sm">
                <SplitItem>
                  <Button
                    variant="secondary"
                    icon={<ExternalLinkAltIcon />}
                    iconPosition="end"
                    component="a"
                    onClick={() => {
                      // start polling for GH key
                      setGhPollEnabled(true);
                    }}
                    href="https://github.com/settings/ssh/new"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    Open GitHub SSH keys
                  </Button>
                </SplitItem>
                <SplitItem>
                  <Button
                    onClick={() => setGhPollEnabled(true)}
                    variant="link"
                    isDisabled={ghPollEnabled}
                    isLoading={ghPollEnabled}
                  >
                    Already added the key? Click here to check if it's detected.
                  </Button>
                </SplitItem>
              </Split>
            )}
          </StackItem>
          <StackItem>
            {ghKeyError && (
              <Alert
                variant="danger"
                isInline
                isPlain
                title="Error checking GitHub key"
                className="pf-v6-u-mt-md"
              >
                {ghKeyError}
              </Alert>
            )}
            {!githubUsername && (
              <Alert
                variant="warning"
                isInline
                isPlain
                title="GitHub username not found in token claims."
                className="pf-v6-u-mt-md"
              >
                You may need to enroll manually after adding the key.
              </Alert>
            )}
          </StackItem>
        </Stack>
      </CardBody>
    </Card>
  );
};

export default GHEnroll;
