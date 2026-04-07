package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
)

type authSetupFlags struct {
	issuerURL                string
	clientID                 string
	scopes                   string
	methodID                 string
	audience                 string
	keyEnrollmentClientID    string
	oidcCAFile               string
	registryID               string
	registrySubjectExpr      string
}

func newAuthSetupCmd(ctx *cmdContext) *cobra.Command {
	f := &authSetupFlags{}
	cmd := &cobra.Command{
		Use:   "setup",
		Short: "Configure OIDC authentication on the server and locally",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAuthSetup(cmd, ctx, f)
		},
	}
	cmd.Flags().StringVar(&f.issuerURL, "issuer-url", "", "OIDC issuer URL (required)")
	cmd.Flags().StringVar(&f.clientID, "client-id", "", "OAuth2 client ID (required)")
	cmd.Flags().StringVar(&f.scopes, "scopes", "openid,profile,email", "Comma-separated OAuth2 scopes")
	cmd.Flags().StringVar(&f.methodID, "method-id", "default", "Auth method ID on the server")
	cmd.Flags().StringVar(&f.audience, "audience", "", "Expected audience claim")
	cmd.Flags().StringVar(&f.keyEnrollmentClientID, "key-enrollment-client-id", "", "OAuth2 client ID for signing key enrollment (dedicated OIDC client)")
	cmd.Flags().StringVar(&f.oidcCAFile, "oidc-ca-file", "", "PEM CA certificate for OIDC issuer (saved to local config)")
	cmd.Flags().StringVar(&f.registryID, "registry-id", "", "External key registry ID (e.g. github.com)")
	cmd.Flags().StringVar(&f.registrySubjectExpr, "registry-subject-expression", "", "CEL expression mapping ID token claims to a registry subject (e.g. claims.preferred_username)")
	_ = cmd.MarkFlagRequired("issuer-url")
	_ = cmd.MarkFlagRequired("client-id")
	return cmd
}

func runAuthSetup(cmd *cobra.Command, ctx *cmdContext, f *authSetupFlags) error {
	client := pb.NewAuthMethodServiceClient(ctx.conn)

	oidcConfig := &pb.OIDCConfig{
		IssuerUrl:             f.issuerURL,
		Audience:              f.audience,
		KeyEnrollmentAudience: f.keyEnrollmentClientID,
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

	scopes := strings.Split(f.scopes, ",")
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
