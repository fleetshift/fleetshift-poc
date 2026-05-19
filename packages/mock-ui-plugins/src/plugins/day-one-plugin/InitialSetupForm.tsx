import { useCallback, useEffect, useRef, useState } from "react";
import {
  ActionGroup,
  Alert,
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  ClipboardCopy,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  Divider,
  Form,
  FormGroup,
  FormSection,
  HelperText,
  HelperTextItem,
  Icon,
  Radio,
  Spinner,
  TextInput,
  Title,
} from "@patternfly/react-core";
import { CheckCircleIcon } from "@patternfly/react-icons";
import "./SetupPage.scss";

type BackingStore = "sqlite" | "postgres";
type KeyRegistry = "oidc" | "github";

interface OidcConfig {
  issuerUrl: string;
  audience: string;
  authorizationEndpoint: string;
  tokenEndpoint: string;
  jwksUri: string;
  registrySubjectMapping?: {
    registryId: string;
    expression: string;
  };
}

interface AuthMethod {
  name: string;
  type: string;
  oidcConfig: OidcConfig;
}

type AuthState =
  | { status: "idle" }
  | { status: "submitting" }
  | { status: "configured"; authMethod: AuthMethod }
  | { status: "error"; message: string };

async function fetchAuthMethod(): Promise<AuthMethod | null> {
  const res = await fetch("/v1/authMethods/default");
  if (res.status === 404 || res.status === 500) return null;
  if (!res.ok) throw new Error(`Unexpected status ${res.status}`);
  return res.json();
}

async function triggerAuthSetup(
  issuerUrl: string,
  audience: string,
  keyRegistry: KeyRegistry,
): Promise<void> {
  const oidcConfig: Record<string, unknown> = {
    issuer_url: issuerUrl.replace(/\/+$/, ""),
    audience,
    key_enrollment_audience: audience,
  };

  if (keyRegistry === "github") {
    oidcConfig.registry_subject_mapping = {
      registry_id: "github.com",
      expression: "claims.github_username",
    };
  } else {
    oidcConfig.public_key_claim_expression = "claims.signing_public_key";
  }

  await fetch("/v1/authMethods?auth_method_id=default", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ type: "TYPE_OIDC", oidc_config: oidcConfig }),
  });
}

interface SetupWsCallbacks {
  onCreated: (method: AuthMethod) => void;
  onFailed: (message: string) => void;
}

function useSetupWebSocket(callbacks: SetupWsCallbacks) {
  const cbRef = useRef(callbacks);
  cbRef.current = callbacks;

  useEffect(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(
      `${protocol}//${window.location.host}/api/ui/setup/ws`,
    );

    ws.onmessage = (ev) => {
      try {
        const event = JSON.parse(ev.data);
        if (event.type === "auth_method_created" && event.auth_method) {
          cbRef.current.onCreated(event.auth_method);
        } else if (event.type === "auth_method_failed" && event.error) {
          cbRef.current.onFailed(event.error);
        }
      } catch {
        // ignore malformed messages
      }
    };

    return () => ws.close();
  }, []);
}

const AuthConfiguredBanner = ({ authMethod }: { authMethod: AuthMethod }) => {
  const oidc = authMethod.oidcConfig;
  return (
    <Card isCompact className="fs-setup__configured-card">
      <CardHeader>
        <CardTitle>
          <Icon status="success">
            <CheckCircleIcon />
          </Icon>{" "}
          Authentication configured
        </CardTitle>
      </CardHeader>
      <CardBody>
        <DescriptionList isCompact isHorizontal>
          <DescriptionListGroup>
            <DescriptionListTerm>Issuer</DescriptionListTerm>
            <DescriptionListDescription>
              {oidc.issuerUrl}
            </DescriptionListDescription>
          </DescriptionListGroup>
          <DescriptionListGroup>
            <DescriptionListTerm>Audience</DescriptionListTerm>
            <DescriptionListDescription>
              {oidc.audience || "—"}
            </DescriptionListDescription>
          </DescriptionListGroup>
          {oidc.registrySubjectMapping && (
            <DescriptionListGroup>
              <DescriptionListTerm>Key registry</DescriptionListTerm>
              <DescriptionListDescription>
                {oidc.registrySubjectMapping.registryId}
              </DescriptionListDescription>
            </DescriptionListGroup>
          )}
        </DescriptionList>
      </CardBody>
    </Card>
  );
};

const SetupPage = () => {
  const [pageLoading, setPageLoading] = useState(true);
  const [authState, setAuthState] = useState<AuthState>({ status: "idle" });
  const [backingStore, setBackingStore] = useState<BackingStore>("sqlite");
  const [issuerUrl, setIssuerUrl] = useState("");
  const [audience, setAudience] = useState("");
  const [keyRegistry, setKeyRegistry] = useState<KeyRegistry>("oidc");

  useEffect(() => {
    fetchAuthMethod().then((method) => {
      if (method) {
        setAuthState({ status: "configured", authMethod: method });
      }
      setPageLoading(false);
    });
  }, []);

  useSetupWebSocket({
    onCreated: (method) => {
      setAuthState({ status: "configured", authMethod: method });
    },
    onFailed: (message) => {
      setAuthState({ status: "error", message });
    },
  });

  const handleConfigure = useCallback(() => {
    setAuthState({ status: "submitting" });
    triggerAuthSetup(issuerUrl, audience, keyRegistry);
  }, [issuerUrl, audience, keyRegistry]);

  if (pageLoading) {
    return <Spinner aria-label="Loading setup state" />;
  }

  const authConfigured = authState.status === "configured";

  return (
    <div className="fs-setup">
      <Title headingLevel="h1" className="fs-setup__title">
        Initial OpenShift Management Engine Setup
      </Title>
      <p className="fs-setup__subtitle">
        Configure your console settings before first launch.
      </p>

      <Form>
        <FormSection title="Backing store">
          <p>
            Select the database the console uses to persist configuration and
            operational state.
          </p>
          <Radio
            id="store-sqlite"
            name="backing-store"
            label="SQLite"
            description="Lightweight embedded database, good for development and small deployments."
            isChecked={backingStore === "sqlite"}
            onChange={() => setBackingStore("sqlite")}
          />
          <Radio
            id="store-postgres"
            name="backing-store"
            label="PostgreSQL"
            description="Production-grade relational database for larger deployments."
            isChecked={backingStore === "postgres"}
            onChange={() => setBackingStore("postgres")}
          />
        </FormSection>

        <Divider />

        <FormSection title="Authentication provider">
          <p>Configure your OIDC identity provider for user sign-in.</p>

          {authConfigured ? (
            <AuthConfiguredBanner authMethod={authState.authMethod} />
          ) : (
            <>
              {authState.status === "error" && (
                <Alert
                  variant="danger"
                  isInline
                  title="Configuration failed"
                  className="fs-setup__alert"
                >
                  {authState.message}
                </Alert>
              )}
              {authState.status === "submitting" && (
                <Alert
                  variant="info"
                  isInline
                  title="Configuring authentication"
                  className="fs-setup__alert"
                >
                  Connecting to your identity provider. This may take a moment.
                </Alert>
              )}
              <FormGroup label="Issuer URL" isRequired fieldId="issuer-url">
                <TextInput
                  id="issuer-url"
                  value={issuerUrl}
                  isDisabled={authState.status === "submitting"}
                  onChange={(_e, v) => setIssuerUrl(v)}
                  placeholder="https://keycloak.example.com/realms/my-realm"
                />
              </FormGroup>
              <FormGroup label="Audience" isRequired fieldId="audience">
                <TextInput
                  id="audience"
                  value={audience}
                  isDisabled={authState.status === "submitting"}
                  onChange={(_e, v) => setAudience(v)}
                  placeholder="fleetshift"
                />
                <HelperText>
                  <HelperTextItem>
                    The expected <code>aud</code> claim in OIDC tokens. Must
                    match the resource server audience configured in your
                    identity provider.
                  </HelperTextItem>
                </HelperText>
              </FormGroup>
              <FormGroup
                label="Signing public key registry"
                fieldId="key-registry"
              >
                <HelperText>
                  <HelperTextItem>
                    Choose where verification public keys are stored for signed
                    artifacts and policies.
                  </HelperTextItem>
                </HelperText>
                <Radio
                  id="keys-oidc"
                  name="key-registry"
                  label="OIDC-hosted keys"
                  description="Signing public keys are discovered through your OIDC identity provider via token claims. Uses your existing IdP trust."
                  isChecked={keyRegistry === "oidc"}
                  isDisabled={authState.status === "submitting"}
                  onChange={() => setKeyRegistry("oidc")}
                />
                <Radio
                  id="keys-github"
                  name="key-registry"
                  label="GitHub SSH signing keys"
                  description="Keys are fetched from GitHub user profiles. Requires users to publish SSH signing keys on their GitHub account."
                  isChecked={keyRegistry === "github"}
                  isDisabled={authState.status === "submitting"}
                  onChange={() => setKeyRegistry("github")}
                />
              </FormGroup>
              <FormGroup label="CLI equivalent" fieldId="cli-command">
                <HelperText>
                  <HelperTextItem>
                    Or configure from the command line:
                  </HelperTextItem>
                </HelperText>
                <ClipboardCopy isReadOnly isCode>
                  {`fleetctl auth setup --issuer-url=${issuerUrl || "<URL>"} --client-id=fleetshift-cli --audience=${audience || "<AUDIENCE>"} --key-enrollment-client-id=${audience || "<AUDIENCE>"}${keyRegistry === "github" ? " --registry-id=github.com --registry-subject-expression=claims.github_username" : ""}`}
                </ClipboardCopy>
              </FormGroup>
              <ActionGroup>
                <Button
                  variant="primary"
                  isDisabled={
                    !issuerUrl || !audience || authState.status === "submitting"
                  }
                  isLoading={authState.status === "submitting"}
                  onClick={handleConfigure}
                >
                  Configure
                </Button>
              </ActionGroup>
            </>
          )}
        </FormSection>

        <Divider />

        <FormSection title="OIDC claim mapping">
          <p>Choose which OIDC token claim supplies the console username.</p>
          <p className="fs-setup__info-text">
            Using platform defaults. Custom mapping will be available in a
            future release.
          </p>
        </FormSection>

        <Divider />

        <ActionGroup>
          <Button
            variant="primary"
            size={authConfigured ? "lg" : undefined}
            isDisabled={!authConfigured}
            component="a"
            href="/"
          >
            Continue to console
          </Button>
        </ActionGroup>
      </Form>
    </div>
  );
};

export default SetupPage;
