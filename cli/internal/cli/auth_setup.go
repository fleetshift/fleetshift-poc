package cli

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
)

// authSetupFlags holds flag values for [newAuthSetupCmd].
type authSetupFlags struct {
	localConfig           bool
	configureServer       bool
	issuerURL             string
	clientID              string
	scopes                string
	oidcCAFile            string
	keyEnrollmentClientID string

	// Server AuthMethod fields (only with --configure-server).
	methodID            string
	audience            string
	publicKeyClaimExpr  string
	registryID          string
	registrySubjectExpr string
}

// serverOnlyAuthSetupFlags are only valid with --configure-server.
var serverOnlyAuthSetupFlags = []string{
	"method-id",
	"audience",
	"public-key-claim-expression",
	"registry-id",
	"registry-subject-expression",
}

// newAuthSetupCmd builds the `fleetctl auth setup` command.
//
// By default (and with --local-config) it discovers OIDC endpoints and writes
// local Fleetctl auth config without talking to the FleetShift server.
//
// --configure-server restores the historical CreateAuthMethod + local config
// path for a future IdP create/update API. Until that API exists, the server
// rejects public AuthMethod create.
func newAuthSetupCmd(ctx *cmdContext) *cobra.Command {
	f := &authSetupFlags{}
	cmd := &cobra.Command{
		Use:   "setup",
		Short: "Configure Fleetctl OIDC client settings (and optionally the server AuthMethod)",
		Long: `Write local Fleetctl OIDC client configuration to ~/.config/fleetshift/auth.json
for use by auth login and auth enroll-signing.

Default mode (also --local-config):
  Discover authorization and token endpoints from --issuer-url and write
  local auth.json. Does not create or change server authentication settings.

--configure-server:
  Call CreateAuthMethod on the FleetShift server, then write local auth.json
  from the response. Intended for a future IdP create/update API. Public
  AuthMethod create is currently disabled on the server (config bootstrap
  installs the first IdP; mutate remains frozen until an IdP-update API
  exists), so this mode fails against current servers.`,
		// Always skip the root dialer; local mode needs no connection, and
		// --configure-server dials explicitly so the mode can stay optional.
		Annotations: map[string]string{
			annotationSkipServer: "true",
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAuthSetup(cmd, ctx, f)
		},
	}
	cmd.Flags().BoolVar(&f.localConfig, "local-config", false, "Write local auth.json only (default behavior; optional explicit alias)")
	cmd.Flags().BoolVar(&f.configureServer, "configure-server", false, "Create/update the server AuthMethod, then write local auth.json (deferred until IdP update API)")
	cmd.Flags().StringVar(&f.issuerURL, "issuer-url", "", "OIDC issuer URL (required)")
	cmd.Flags().StringVar(&f.clientID, "client-id", "", "OAuth2 client ID for Fleetctl login (required; e.g. fleetshift-cli)")
	cmd.Flags().StringVar(&f.scopes, "scopes", "openid,profile,email", "Comma-separated OAuth2 scopes for login")
	cmd.Flags().StringVar(&f.keyEnrollmentClientID, "key-enrollment-client-id", "", "OAuth2 client ID for signing key enrollment (e.g. fleetshift-signing)")
	cmd.Flags().StringVar(&f.oidcCAFile, "oidc-ca-file", "", "PEM CA certificate for the OIDC issuer (used for discovery TLS and saved to local config)")
	cmd.Flags().StringVar(&f.methodID, "method-id", "default", "Auth method ID on the server (requires --configure-server)")
	cmd.Flags().StringVar(&f.audience, "audience", "", "Expected audience claim on the server AuthMethod (requires --configure-server)")
	cmd.Flags().StringVar(&f.publicKeyClaimExpr, "public-key-claim-expression", "", "CEL expression extracting the signer's SPKI public key from ID token claims (requires --configure-server)")
	cmd.Flags().StringVar(&f.registryID, "registry-id", "", "External key registry ID (e.g. github.com; requires --configure-server)")
	cmd.Flags().StringVar(&f.registrySubjectExpr, "registry-subject-expression", "", "CEL expression mapping ID token claims to a registry subject (requires --configure-server)")
	_ = cmd.MarkFlagRequired("issuer-url")
	_ = cmd.MarkFlagRequired("client-id")
	return cmd
}

// runAuthSetup dispatches local or server AuthMethod setup from the selected flags.
func runAuthSetup(cmd *cobra.Command, ctx *cmdContext, f *authSetupFlags) error {
	if f.localConfig && f.configureServer {
		return fmt.Errorf("--local-config and --configure-server are mutually exclusive")
	}
	if !f.configureServer {
		for _, name := range serverOnlyAuthSetupFlags {
			if cmd.Flags().Changed(name) {
				return fmt.Errorf("--%s requires --configure-server", name)
			}
		}
		return runAuthSetupLocal(cmd, f)
	}
	return runAuthSetupServer(cmd, ctx, f)
}

// runAuthSetupLocal discovers OIDC endpoints for the issuer and saves local
// auth config under ~/.config/fleetshift/auth.json.
func runAuthSetupLocal(cmd *cobra.Command, f *authSetupFlags) error {
	cfg := auth.Config{
		IssuerURL:             strings.TrimSpace(f.issuerURL),
		ClientID:              strings.TrimSpace(f.clientID),
		OIDCCAFile:            f.oidcCAFile,
		KeyEnrollmentClientID: strings.TrimSpace(f.keyEnrollmentClientID),
	}

	httpClient, err := cfg.HTTPClient()
	if err != nil {
		return fmt.Errorf("OIDC CA client: %w", err)
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	endpoints, err := auth.DiscoverEndpoints(cmd.Context(), cfg.IssuerURL, httpClient)
	if err != nil {
		return err
	}

	scopes := splitScopes(f.scopes)
	if len(scopes) == 0 {
		return fmt.Errorf("--scopes must include at least one scope")
	}

	cfg.Scopes = scopes
	cfg.AuthorizationEndpoint = endpoints.AuthorizationEndpoint
	cfg.TokenEndpoint = endpoints.TokenEndpoint

	if err := auth.SaveConfig(cfg); err != nil {
		return fmt.Errorf("save local config: %w", err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Local authentication configured. Run 'fleetctl auth login' to authenticate.\n")
	return nil
}

// runAuthSetupServer creates an AuthMethod on the FleetShift server, then
// writes local auth.json from the CreateAuthMethod response. Public create is
// currently rejected by the server; this path is retained for the future IdP
// create/update API.
func runAuthSetupServer(cmd *cobra.Command, ctx *cmdContext, f *authSetupFlags) error {
	conn, err := dial(ctx.flags)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewAuthMethodServiceClient(conn)

	oidcConfig := &pb.OIDCConfig{
		IssuerUrl:                f.issuerURL,
		Audience:                 f.audience,
		KeyEnrollmentAudience:    f.keyEnrollmentClientID,
		PublicKeyClaimExpression: f.publicKeyClaimExpr,
	}
	if f.registryID != "" && f.registrySubjectExpr != "" {
		oidcConfig.RegistrySubjectMapping = &pb.RegistrySubjectMapping{
			RegistryId: f.registryID,
			Expression: f.registrySubjectExpr,
		}
	}

	resp, err := client.CreateAuthMethod(cmd.Context(), &pb.CreateAuthMethodRequest{
		AuthMethodId: f.methodID,
		AuthMethod: &pb.AuthMethod{
			Type:       pb.AuthMethod_TYPE_OIDC,
			OidcConfig: oidcConfig,
		},
	})
	if err != nil {
		return fmt.Errorf("create auth method on server: %w", err)
	}

	scopes := splitScopes(f.scopes)
	if len(scopes) == 0 {
		return fmt.Errorf("--scopes must include at least one scope")
	}

	cfg := auth.Config{
		IssuerURL:             f.issuerURL,
		ClientID:              f.clientID,
		Scopes:                scopes,
		AuthorizationEndpoint: resp.GetOidcConfig().GetAuthorizationEndpoint(),
		TokenEndpoint:         resp.GetOidcConfig().GetTokenEndpoint(),
		KeyEnrollmentClientID: f.keyEnrollmentClientID,
		OIDCCAFile:            f.oidcCAFile,
	}

	if err := auth.SaveConfig(cfg); err != nil {
		return fmt.Errorf("save local config: %w", err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Authentication configured. Run 'fleetctl auth login' to authenticate.\n")
	return nil
}

// splitScopes splits a comma-separated scope list, trimming whitespace and
// dropping empty entries.
func splitScopes(spec string) []string {
	var out []string
	for _, s := range strings.Split(spec, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
