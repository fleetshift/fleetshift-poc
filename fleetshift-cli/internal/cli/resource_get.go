package cli

import (
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/dynamic"
	"github.com/spf13/cobra"
)

func newResourceGetCmd(ctx *cmdContext) *cobra.Command {
	return &cobra.Command{
		Use:   "get <type> <id>",
		Short: "Get a managed resource by id",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			plural := args[0]
			id := args[1]

			client := dynamic.NewClient(ctx.conn)
			rt, err := client.ResolveType(cmd.Context(), plural)
			if err != nil {
				return err
			}

			resp, err := client.Get(cmd.Context(), rt, id)
			if err != nil {
				return fmt.Errorf("get %s/%s: %w", plural, id, err)
			}

			return ctx.printer.PrintResource(resp, resourceColumns())
		},
	}
}
