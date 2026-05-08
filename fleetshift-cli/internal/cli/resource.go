package cli

import "github.com/spf13/cobra"

func newResourceCmd(ctx *cmdContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "resource",
		Aliases: []string{"res"},
		Short:   "Interact with addon-provided managed resources",
	}

	cmd.AddCommand(newResourceTypesCmd(ctx))
	cmd.AddCommand(newResourceDescribeCmd(ctx))
	cmd.AddCommand(newResourceCreateCmd(ctx))
	cmd.AddCommand(newResourceGetCmd(ctx))
	cmd.AddCommand(newResourceListCmd(ctx))
	cmd.AddCommand(newResourceDeleteCmd(ctx))

	return cmd
}
