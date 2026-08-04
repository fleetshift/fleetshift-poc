package cli

import "github.com/spf13/cobra"

func newResourceCmd(ctx *cmdContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "resource",
		Aliases: []string{"res"},
		Short:   "Manage and query addon-provided managed resources",
	}

	cmd.PersistentFlags().String("service", "", "gRPC service name to disambiguate when multiple services expose the same collection")

	cmd.AddCommand(newResourceTypesCmd(ctx))
	cmd.AddCommand(newResourceDescribeCmd(ctx))
	cmd.AddCommand(newResourceCreateCmd(ctx))
	cmd.AddCommand(newResourceGetCmd(ctx))
	cmd.AddCommand(newResourceListCmd(ctx))
	cmd.AddCommand(newResourceQueryCmd(ctx))
	cmd.AddCommand(newResourceDeleteCmd(ctx))

	return cmd
}

// serviceFlag reads the --service persistent flag from a command.
func serviceFlag(cmd *cobra.Command) string {
	s, _ := cmd.Flags().GetString("service")
	return s
}
