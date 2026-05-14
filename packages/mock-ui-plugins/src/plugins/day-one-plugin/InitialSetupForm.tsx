import { useState } from "react";
import {
  ActionGroup,
  Alert,
  Button,
  ClipboardCopy,
  Divider,
  Form,
  FormGroup,
  FormSection,
  HelperText,
  HelperTextItem,
  Radio,
  TextInput,
  Title,
} from "@patternfly/react-core";

type BackingStore = "sqlite" | "postgres";
type KeyRegistry = "oidc" | "github";

export default function InitialSetupForm() {
  const [backingStore, setBackingStore] = useState<BackingStore>("sqlite");
  const [issuerUrl, setIssuerUrl] = useState("");
  const [clientId, setClientId] = useState("");
  const [keyRegistry, setKeyRegistry] = useState<KeyRegistry>("oidc");

  return (
    <div style={{ maxWidth: 800 }}>
      <Title
        headingLevel="h1"
        style={{ marginBottom: "var(--pf-t--global--spacer--sm)" }}
      >
        Initial OpenShift Management Engine Setup
      </Title>
      <p style={{ marginBottom: "var(--pf-t--global--spacer--xl)" }}>
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
          <p>
            Configure your OIDC identity provider for user sign-in.
          </p>
          <FormGroup label="Issuer URL" isRequired fieldId="issuer-url">
            <TextInput
              id="issuer-url"
              value={issuerUrl}
              onChange={(_e, v) => setIssuerUrl(v)}
              placeholder="https://keycloak.example.com/realms/my-realm"
            />
          </FormGroup>
          <FormGroup label="Client ID" isRequired fieldId="client-id">
            <TextInput
              id="client-id"
              value={clientId}
              onChange={(_e, v) => setClientId(v)}
              placeholder="fleetshift-ui"
            />
          </FormGroup>
          <FormGroup label="Redirect URI" fieldId="redirect-uri">
            <ClipboardCopy isReadOnly>
              {`${window.location.origin}/api/auth/callback`}
            </ClipboardCopy>
          </FormGroup>
          <FormGroup label="CLI equivalent" fieldId="cli-command">
            <HelperText>
              <HelperTextItem>
                Or configure from the command line:
              </HelperTextItem>
            </HelperText>
            <ClipboardCopy isReadOnly isCode>
              {`fleetctl auth setup --issuer-url=${issuerUrl || "<URL>"} --client-id=${clientId || "<ID>"}`}
            </ClipboardCopy>
          </FormGroup>
          <Button
            variant="secondary"
            isDisabled={!issuerUrl || !clientId}
            size="sm"
          >
            Test connection
          </Button>
        </FormSection>

        <Divider />

        <FormSection title="Signing public key registry">
          <p>
            Choose where verification public keys are stored for signed
            artifacts and policies.
          </p>
          <Radio
            id="keys-oidc"
            name="key-registry"
            label="OIDC-hosted keys"
            description="Signing public keys are discovered through your OIDC identity provider via token claims. Uses your existing IdP trust."
            isChecked={keyRegistry === "oidc"}
            onChange={() => setKeyRegistry("oidc")}
          />
          <Radio
            id="keys-github"
            name="key-registry"
            label="GitHub SSH signing keys"
            description="Keys are fetched from GitHub user profiles. Requires users to publish SSH signing keys on their GitHub account."
            isChecked={keyRegistry === "github"}
            onChange={() => setKeyRegistry("github")}
          />
        </FormSection>

        <Divider />

        <FormSection title="OIDC claim mapping">
          <p>Choose which OIDC token claim supplies the console username.</p>
          <p style={{ color: "var(--pf-t--global--color--status--info--default)" }}>
            Using platform defaults. Custom mapping will be available in a
            future release.
          </p>
        </FormSection>

        <Divider />

        <Alert
          variant="info"
          isInline
          title="Console restart"
          style={{ marginBottom: "var(--pf-t--global--spacer--md)" }}
        >
          The console will restart after you press OK. You will need to
          authenticate with your chosen provider.
        </Alert>

        <ActionGroup>
          <Button variant="primary">OK</Button>
          <Button variant="link">Cancel</Button>
        </ActionGroup>
      </Form>
    </div>
  );
}
