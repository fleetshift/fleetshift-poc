import {
  Alert,
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  ClipboardCopy,
  Icon,
  Spinner,
  Title,
} from "@patternfly/react-core";
import { KeyIcon } from "@patternfly/react-icons";
import { AnimatePresence, motion } from "motion/react";
import { useSigningKeyEnrollment } from "./useSigningKeyEnrollment";
import "./SetupPage.scss";
import AnimatedHeight from "./components/AnimatedHeight";
import EnrollmentDL from "./components/EnrollmentDL";
import EnrollmentError from "./components/EnrollmentError";
import GHEnroll from "./components/GHEnroll";
import OIDCEnroll from "./components/OIDCEnroll";

const messages: Record<string, string> = {
  loading: "Loading configuration...",
  generating: "Generating ECDSA P-256 signing key...",
  verifying: "Verifying signature with the server...",
};

const transition = {
  duration: 0.5,
  type: "spring" as const,
  bounce: 0.15,
  filter: { ease: "easeInOut" as const },
};

const SigningKeyEnrollment = () => {
  const {
    step,
    sshPublicKey,
    registry,
    error,
    enrollmentName,
    githubUsername,
    isSetupFlow,
    enrollOidc,
    retry,
    handleReenroll,
    setGhPollEnabled,
    simulateSuccess,
  } = useSigningKeyEnrollment();

  if (step === "loading" || step === "generating" || step === "verifying") {
    return (
      <div className="fs-setup">
        <Title headingLevel="h1" className="fs-setup__title">
          Signing Key Enrollment
        </Title>
        <Spinner aria-label={messages[step]} />
        <p className="pf-v6-u-mt-md">{messages[step]}</p>
      </div>
    );
  }

  if (step === "error") {
    return <EnrollmentError error={error} onRetry={retry} />;
  }

  return (
    <AnimatedHeight className="fs-setup">
      <Title headingLevel="h1" className="fs-setup__title">
        Signing Key Enrollment
      </Title>

      <AnimatePresence mode="popLayout">
        {step === "enrolled" ? (
          <motion.div
            key="enrolled"
            initial={{ opacity: 0, y: 30, filter: "blur(10px)" }}
            animate={{ opacity: 1, y: 0, filter: "blur(0px)" }}
            exit={{ opacity: 0, y: -30, filter: "blur(10px)" }}
            transition={transition}
          >
            <Alert
              variant="success"
              isInline
              title="Signing key enrolled and verified"
            >
              <EnrollmentDL
                registry={registry}
                enrollmentName={enrollmentName}
                sshPublicKey={sshPublicKey}
              />
            </Alert>
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ ...transition, delay: 0.15 }}
              className="pf-v6-u-mt-xl"
            >
              {isSetupFlow && (
                <Button
                  variant="primary"
                  component="a"
                  href="/"
                  className="pf-v6-u-mr-md"
                >
                  Continue to console
                </Button>
              )}
              <Button variant="secondary" onClick={handleReenroll}>
                Re-enroll
              </Button>
            </motion.div>
          </motion.div>
        ) : (
          <motion.div
            key="register"
            initial={{ opacity: 0, y: 30, filter: "blur(10px)" }}
            animate={{ opacity: 1, y: 0, filter: "blur(0px)" }}
            exit={{ opacity: 0, y: -30, filter: "blur(10px)" }}
            transition={transition}
          >
            <p className="fs-setup__subtitle">
              Register your signing key to sign deployments and policies.
            </p>

            <Card isCompact className="pf-v6-u-mt-lg">
              <CardHeader>
                <CardTitle>
                  <Icon>
                    <KeyIcon />
                  </Icon>{" "}
                  Your signing key
                </CardTitle>
              </CardHeader>
              <CardBody>
                {sshPublicKey && (
                  <ClipboardCopy isReadOnly isCode>
                    {sshPublicKey}
                  </ClipboardCopy>
                )}
              </CardBody>
            </Card>

            {registry === "github" && (
              <GHEnroll
                githubUsername={githubUsername}
                setGhPollEnabled={setGhPollEnabled}
                simulateSuccess={simulateSuccess}
              />
            )}

            {registry === "oidc" && (
              <OIDCEnroll step={step} enrollOidc={enrollOidc} />
            )}

            <Button
              variant="link"
              component="a"
              href="/"
              className="pf-v6-u-mt-xl"
            >
              Skip to console
            </Button>
          </motion.div>
        )}
      </AnimatePresence>
    </AnimatedHeight>
  );
};

export default SigningKeyEnrollment;
