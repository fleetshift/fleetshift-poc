package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newAuthLogoutCmd builds the `fleetctl auth logout` command.
func newAuthLogoutCmd(ctx *cmdContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logout",
		Short: "Clear stored authentication tokens",
		Annotations: map[string]string{
			annotationSkipServer: "true",
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := ctx.flags.store().Clear(cmd.Context()); err != nil {
				return fmt.Errorf("clear tokens: %w", err)
			}
			fmt.Fprintln(cmd.OutOrStdout(), "Logged out.")
			return nil
		},
	}
	return cmd
}
