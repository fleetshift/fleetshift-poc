import {
  Alert,
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
} from "@patternfly/react-core";
import { ExternalLinkAltIcon } from "@patternfly/react-icons";

import ghSigningKeyImg from "../assets/gh-signing-screen.png";
import MotionPollingAnimation from "./MotionPollingAnimation";

export type GHEnrollProps = {
  githubUsername?: string | null;
  setGhPollEnabled: (enabled: boolean) => void;
  simulateSuccess?: () => void;
};

const GHEnroll = ({
  githubUsername,
  setGhPollEnabled,
  simulateSuccess,
}: GHEnrollProps) => {
  return (
    <Card isCompact className="pf-v6-u-mt-lg">
      <CardHeader>
        <CardTitle>Register on GitHub</CardTitle>
      </CardHeader>
      <CardBody>
        <p>
          Copy your public key above and add it as a{" "}
          <strong>Signing Key</strong> on GitHub.
        </p>
        <img
          src={ghSigningKeyImg}
          alt="GitHub SSH key settings page — select Signing Key from the key type dropdown"
          className="fs-setup__screenshot"
        />
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
          className="pf-v6-u-mt-sm"
        >
          Open GitHub SSH keys
        </Button>
        {githubUsername ? (
          <>
            <MotionPollingAnimation>
              Waiting for key to appear on GitHub&hellip;
            </MotionPollingAnimation>
            {simulateSuccess && (
              <Button
                variant="link"
                onClick={simulateSuccess}
                className="pf-v6-u-mt-sm"
              >
                Simulate success
              </Button>
            )}
          </>
        ) : (
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
      </CardBody>
    </Card>
  );
};

export default GHEnroll;
