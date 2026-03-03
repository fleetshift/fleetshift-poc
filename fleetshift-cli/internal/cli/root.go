package cli

import (
	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/output"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type globalFlags struct {
	server       string
	outputFormat string
}

type cmdContext struct {
	flags   globalFlags
	conn    *grpc.ClientConn
	printer *output.Printer
}

// New returns the root command for the fleetctl CLI.
func New() *cobra.Command {
	ctx := &cmdContext{}

	root := &cobra.Command{
		Use:          "fleetctl",
		Short:        "FleetShift command-line client",
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			format := output.Format(ctx.flags.outputFormat)
			if err := format.Validate(); err != nil {
				return err
			}
			ctx.printer = output.NewPrinter(cmd.OutOrStdout(), format)

			conn, err := dial(cmd.Context(), ctx.flags.server)
			if err != nil {
				return err
			}
			ctx.conn = conn
			return nil
		},
		PersistentPostRunE: func(_ *cobra.Command, _ []string) error {
			if ctx.conn != nil {
				return ctx.conn.Close()
			}
			return nil
		},
	}

	root.PersistentFlags().StringVarP(&ctx.flags.server, "server", "s", "localhost:50051", "gRPC server address")
	root.PersistentFlags().StringVarP(&ctx.flags.outputFormat, "output", "o", string(output.FormatTable), "output format (table, json)")

	root.AddCommand(newDeploymentCmd(ctx))

	return root
}
