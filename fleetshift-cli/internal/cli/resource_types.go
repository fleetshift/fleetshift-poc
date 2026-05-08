package cli

import (
	"fmt"
	"text/tabwriter"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/dynamic"
	"github.com/spf13/cobra"
)

func newResourceTypesCmd(ctx *cmdContext) *cobra.Command {
	return &cobra.Command{
		Use:   "types",
		Short: "List available managed resource types",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := dynamic.NewClient(ctx.conn)
			types, err := client.ListResourceTypes(cmd.Context())
			if err != nil {
				return err
			}

			if len(types) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No managed resource types available.")
				return nil
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "PLURAL\tSINGULAR\tSERVICE")
			for _, rt := range types {
				fmt.Fprintf(w, "%s\t%s\t%s\n", rt.Plural, rt.Singular, rt.ServiceName)
			}
			return w.Flush()
		},
	}
}
