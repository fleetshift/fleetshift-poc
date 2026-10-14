package cli

import (
	"github.com/spf13/cobra"
)

// New returns the root command for the fleetshift CLI.
func New() *cobra.Command {
	root := &cobra.Command{
		Use:   "fleetshift",
		Short: "FleetShift management plane",
	}

	root.AddCommand(newServeCmd())

	return root
}
