package ocp

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

// OIDCProviderConfig holds configuration for generating OIDC authentication manifests
type OIDCProviderConfig struct {
	IssuerURL       string
	Audiences       []string
	CABundle        []byte // PEM CA cert (optional - nil uses system trust)
	ClientSecret    string // Console OIDC client secret (optional)
	CLIClientID     string // Public client ID for oc CLI (optional)
	ConsoleClientID string // Confidential client ID for web console (optional)
}

// GenerateOIDCManifests generates OpenShift manifests for OIDC authentication.
// Returns a map of filename -> YAML content for injection into openshift-install manifests directory.
func GenerateOIDCManifests(cfg OIDCProviderConfig) (map[string][]byte, error) {
	manifests := make(map[string][]byte)

	// Always generate the Authentication CR
	authYAML, err := generateAuthenticationCR(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to generate Authentication CR: %w", err)
	}
	manifests["cluster-authentication-oidc.yaml"] = authYAML

	// Generate CA ConfigMap if CABundle is provided
	if len(cfg.CABundle) > 0 {
		caYAML, err := generateCAConfigMap(cfg.CABundle)
		if err != nil {
			return nil, fmt.Errorf("failed to generate CA ConfigMap: %w", err)
		}
		manifests["cluster-oidc-ca-configmap.yaml"] = caYAML
	}

	// Generate client Secret if ClientSecret is provided
	if cfg.ClientSecret != "" {
		secretYAML, err := generateClientSecret(cfg.ClientSecret)
		if err != nil {
			return nil, fmt.Errorf("failed to generate client Secret: %w", err)
		}
		manifests["cluster-oidc-client-secret.yaml"] = secretYAML
	}

	// Always generate the router-certs placeholder secret.
	// Workaround: with External OIDC (spec.type: OIDC), the OAuth server is
	// not deployed, so the authentication operator's routercerts controller
	// never syncs the ingress wildcard cert into openshift-authentication.
	// The ConfigObserver still checks for this secret and sets Degraded=True
	// if missing, which blocks the installer's cluster initialization wait.
	// Pre-creating an empty secret satisfies the existence check.
	// See: https://github.com/openshift/cluster-authentication-operator/pull/740
	//      commit 557cdc5 ("routercerts: remove operands if external OIDC config is available")
	manifests["cluster-authentication-router-certs.yaml"] = []byte(routerCertsPlaceholder)

	return manifests, nil
}

const authenticationCRTemplate = `apiVersion: config.openshift.io/v1
kind: Authentication
metadata:
  name: cluster
spec:
  type: OIDC
  webhookTokenAuthenticator: null
  oidcProviders:
  - name: fleetshift-oidc
    issuer:
      issuerURL: {{ .IssuerURL }}
      audiences:
{{- range .Audiences }}
      - {{ . }}
{{- end }}
{{- if .HasCABundle }}
      issuerCertificateAuthority:
        name: fleetshift-oidc-ca
{{- end }}
    claimMappings:
      username:
        claim: email
        prefixPolicy: Prefix
        prefix:
          prefixString: 'oidc:'
      groups:
        claim: groups
        prefix: 'oidc:'
{{- if or .HasCLIClient .HasConsoleClient }}
    oidcClients:
{{- if .HasCLIClient }}
    - clientID: {{ .CLIClientID }}
      componentName: cli
      componentNamespace: openshift-console
{{- end }}
{{- if .HasConsoleClient }}
    - clientID: {{ .ConsoleClientID }}
      clientSecret:
        name: ocp-console-secret
      componentName: console
      componentNamespace: openshift-console
{{- end }}
{{- end }}
`

const caConfigMapTemplate = `apiVersion: v1
kind: ConfigMap
metadata:
  name: fleetshift-oidc-ca
  namespace: openshift-config
data:
  ca-bundle.crt: |
{{ .CABundleIndented }}
`

const clientSecretTemplate = `apiVersion: v1
kind: Secret
metadata:
  name: ocp-console-secret
  namespace: openshift-config
stringData:
  clientSecret: {{ .ClientSecretQuoted }}
`

// routerCertsPlaceholder is an empty Secret that prevents the authentication
// operator's ConfigObserver from going Degraded on External OIDC clusters.
// See GenerateOIDCManifests for full explanation.
const routerCertsPlaceholder = `apiVersion: v1
kind: Secret
metadata:
  name: v4-0-config-system-router-certs
  namespace: openshift-authentication
type: Opaque
`

type authCRData struct {
	IssuerURL       string
	Audiences       []string
	HasCABundle     bool
	HasCLIClient    bool
	CLIClientID     string
	HasConsoleClient bool
	ConsoleClientID  string
}

type caConfigMapData struct {
	CABundleIndented string
}

type clientSecretData struct {
	ClientSecretQuoted string
}

func generateAuthenticationCR(cfg OIDCProviderConfig) ([]byte, error) {
	tmpl, err := template.New("auth").Parse(authenticationCRTemplate)
	if err != nil {
		return nil, err
	}

	data := authCRData{
		IssuerURL:        cfg.IssuerURL,
		Audiences:        cfg.Audiences,
		HasCABundle:      len(cfg.CABundle) > 0,
		HasCLIClient:     cfg.CLIClientID != "",
		CLIClientID:      cfg.CLIClientID,
		HasConsoleClient: cfg.ConsoleClientID != "" && cfg.ClientSecret != "",
		ConsoleClientID:  cfg.ConsoleClientID,
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func generateCAConfigMap(caBundle []byte) ([]byte, error) {
	tmpl, err := template.New("ca").Parse(caConfigMapTemplate)
	if err != nil {
		return nil, err
	}

	data := caConfigMapData{
		CABundleIndented: indentPEM(string(caBundle), 4),
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func generateClientSecret(clientSecret string) ([]byte, error) {
	tmpl, err := template.New("secret").Parse(clientSecretTemplate)
	if err != nil {
		return nil, err
	}

	// Use yaml.Marshal to produce a properly quoted/escaped scalar,
	// then trim the trailing newline it appends.
	quoted, err := yaml.Marshal(clientSecret)
	if err != nil {
		return nil, fmt.Errorf("marshal client secret: %w", err)
	}

	data := clientSecretData{
		ClientSecretQuoted: strings.TrimSpace(string(quoted)),
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// indentPEM indents PEM content by the specified number of spaces
func indentPEM(pem string, spaces int) string {
	indent := strings.Repeat(" ", spaces)
	lines := strings.Split(strings.TrimSpace(pem), "\n")
	var indented []string
	for _, line := range lines {
		indented = append(indented, indent+line)
	}
	return strings.Join(indented, "\n")
}
