package cli

import (
	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/spf13/cobra"
)

func newDeploymentDeleteCmd(ctx *cmdContext) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a deployment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := pb.NewDeploymentServiceClient(ctx.conn)

			dep, err := client.DeleteDeployment(cmd.Context(), &pb.DeleteDeploymentRequest{
				Name: qualifyDeploymentName(args[0]),
			})
			if err != nil {
				return err
			}

			return ctx.printer.PrintResource(dep, deploymentColumns())
		},
	}
}
