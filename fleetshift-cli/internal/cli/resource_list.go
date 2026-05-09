package cli

import (
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/dynamic"
	"github.com/spf13/cobra"
)

func newResourceListCmd(ctx *cmdContext) *cobra.Command {
	var pageSize int32

	cmd := &cobra.Command{
		Use:   "list <type>",
		Short: "List managed resources of a type",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			plural := args[0]

			client := dynamic.NewClient(ctx.conn)
			rt, err := client.ResolveType(cmd.Context(), plural)
			if err != nil {
				return err
			}

			msgs, err := client.List(cmd.Context(), rt, pageSize)
			if err != nil {
				return fmt.Errorf("list %s: %w", plural, err)
			}

			return ctx.printer.PrintResourceList(msgs, resourceColumns())
		},
	}

	cmd.Flags().Int32Var(&pageSize, "page-size", 0, "maximum number of results to return")

	return cmd
}
