package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

// newAuthLoginCmd builds the `fleetctl auth login` command.
func newAuthLoginCmd(ctx *cmdContext) *cobra.Command {
	var noBrowser bool
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Authenticate with the configured OIDC provider",
		Annotations: map[string]string{
			annotationSkipServer: "true",
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAuthLogin(cmd, ctx, noBrowser)
		},
	}
	cmd.Flags().BoolVar(&noBrowser, "no-browser", false, "Print AUTH_URL and wait for the callback without opening a system browser")
	return cmd
}

// runAuthLogin loads local OIDC config, runs the loopback OIDC code flow, and
// saves tokens. When noBrowser is true, it prints AUTH_URL and does not call
// openBrowser.
func runAuthLogin(cmd *cobra.Command, ctx *cmdContext, noBrowser bool) error {
	cfg, err := ctx.flags.loadConfig()
	if err != nil {
		return fmt.Errorf("load auth config (run 'fleetctl auth setup' first): %w", err)
	}

	tok, err := runLoopbackOIDC(cmd, cfg, oauthConfig(cfg), noBrowser, oidcLoginFailStatus, oidcLoginSuccessHTML)
	if err != nil {
		return err
	}
	if err := ctx.flags.store().Save(cmd.Context(), auth.TokensFrom(tok)); err != nil {
		return fmt.Errorf("save tokens: %w", err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Login successful!\n")
	return nil
}
