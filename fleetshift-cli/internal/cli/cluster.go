package cli

import "github.com/spf13/cobra"

func newClusterCmd(ctx *cmdContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Manage cluster access",
	}

	cmd.AddCommand(
		newClusterLoginCmd(ctx),
		newClusterTokenCmd(ctx),
	)

	return cmd
}
