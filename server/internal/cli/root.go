package cli

import (
	"github.com/spf13/cobra"
)

// New returns the root command for the resource manager.
func New() *cobra.Command {
	root := &cobra.Command{
		Use:   "resource-manager",
		Short: "Resource Manager management plane",
	}

	root.AddCommand(newServeCmd())

	return root
}
