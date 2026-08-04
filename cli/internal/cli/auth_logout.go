package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func newAuthLogoutCmd(_ *cmdContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logout",
		Short: "Clear stored authentication tokens",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store := auth.KeyringTokenStore{}
			if err := store.Clear(cmd.Context()); err != nil {
				return fmt.Errorf("clear tokens: %w", err)
			}
			fmt.Fprintln(cmd.OutOrStdout(), "Logged out.")
			return nil
		},
	}
	return cmd
}
