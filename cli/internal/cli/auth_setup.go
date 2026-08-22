package cli

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

// authSetupFlags holds flag values for [newAuthSetupCmd].
type authSetupFlags struct {
	issuerURL             string
	clientID              string
	scopes                string
	oidcCAFile            string
	keyEnrollmentClientID string
}

// newAuthSetupCmd builds the `fleetctl auth setup` command.
//
// It discovers OIDC endpoints and writes local Fleetctl auth config without
// talking to the FleetShift server. Initial AuthMethod install is via serve
// OIDC config.
func newAuthSetupCmd(ctx *cmdContext) *cobra.Command {
	f := &authSetupFlags{}
	cmd := &cobra.Command{
		Use:   "setup",
		Short: "Configure Fleetctl OIDC client settings",
		Long: `Write local Fleetctl OIDC client configuration (auth.json) for use by
auth login and auth enroll-signing. The default path is
~/.config/fleetshift/auth.json; --config-dir writes auth.json there instead.

Discover authorization and token endpoints from --issuer-url and write
local auth.json. Does not create or change server authentication settings.

Initial AuthMethod install is via fleetshift serve OIDC config.`,
		Annotations: map[string]string{
			annotationSkipServer: "true",
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAuthSetup(cmd, ctx, f)
		},
	}
	cmd.Flags().StringVar(&f.issuerURL, "issuer-url", "", "OIDC issuer URL (required)")
	cmd.Flags().StringVar(&f.clientID, "client-id", "", "OAuth2 client ID for Fleetctl login (required; e.g. fleetshift-cli)")
	cmd.Flags().StringVar(&f.scopes, "scopes", "openid,profile,email", "Comma-separated OAuth2 scopes for login")
	cmd.Flags().StringVar(&f.keyEnrollmentClientID, "key-enrollment-client-id", "", "OAuth2 client ID for signing key enrollment (e.g. fleetshift-signing)")
	cmd.Flags().StringVar(&f.oidcCAFile, "oidc-ca-file", "", "PEM CA certificate for the OIDC issuer (used for discovery TLS and saved to local config as an absolute path)")
	_ = cmd.MarkFlagRequired("issuer-url")
	_ = cmd.MarkFlagRequired("client-id")
	return cmd
}

// runAuthSetup validates flags, discovers OIDC endpoints, and writes local
// auth config. When --config-dir is set, auth.json lives there. --oidc-ca-file
// is stored as an absolute path so later commands do not depend on CWD.
func runAuthSetup(cmd *cobra.Command, ctx *cmdContext, f *authSetupFlags) error {
	f.issuerURL = strings.TrimSpace(f.issuerURL)
	if f.issuerURL == "" {
		return fmt.Errorf("--issuer-url is required")
	}
	f.clientID = strings.TrimSpace(f.clientID)
	if f.clientID == "" {
		return fmt.Errorf("--client-id is required")
	}
	f.keyEnrollmentClientID = strings.TrimSpace(f.keyEnrollmentClientID)
	f.oidcCAFile = strings.TrimSpace(f.oidcCAFile)
	scopes := splitScopes(f.scopes)
	if len(scopes) == 0 {
		return fmt.Errorf("--scopes must include at least one scope")
	}

	if f.oidcCAFile != "" {
		abs, err := filepath.Abs(f.oidcCAFile)
		if err != nil {
			return fmt.Errorf("resolve --oidc-ca-file: %w", err)
		}
		f.oidcCAFile = abs
	}

	cfg := auth.Config{
		IssuerURL:             f.issuerURL,
		ClientID:              f.clientID,
		OIDCCAFile:            f.oidcCAFile,
		KeyEnrollmentClientID: f.keyEnrollmentClientID,
	}

	httpClient, err := cfg.HTTPClient()
	if err != nil {
		return fmt.Errorf("OIDC CA client: %w", err)
	}

	endpoints, err := auth.DiscoverEndpoints(cmd.Context(), cfg.IssuerURL, httpClient)
	if err != nil {
		return err
	}

	cfg.Scopes = scopes
	cfg.AuthorizationEndpoint = endpoints.AuthorizationEndpoint
	cfg.TokenEndpoint = endpoints.TokenEndpoint

	if err := ctx.flags.saveConfig(cfg); err != nil {
		return fmt.Errorf("save local config: %w", err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Local authentication configured. Run 'fleetctl auth login' to authenticate.\n")
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
