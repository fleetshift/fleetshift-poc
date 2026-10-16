package cli

import (
	"github.com/spf13/cobra"
)

// New returns the root command for the resource manager server.
func New() *cobra.Command {
	root := &cobra.Command{
		Use:   "resource-manager",
		Short: "API and orchestration server for OCM Resource Manager",
	}

	root.AddCommand(newServeCmd())

	return root
}
