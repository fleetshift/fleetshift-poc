package cli

import "github.com/spf13/cobra"

func newAuthCmd(ctx *cmdContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Manage authentication",
	}

	cmd.AddCommand(
		newAuthSetupCmd(ctx),
		newAuthLoginCmd(ctx),
		newAuthLogoutCmd(ctx),
		newAuthInspectTokenCmd(ctx),
		newAuthEnrollSigningCmd(ctx),
	)

	return cmd
}
