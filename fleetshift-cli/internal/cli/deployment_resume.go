package cli

import (
	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/spf13/cobra"
)

func newDeploymentResumeCmd(ctx *cmdContext) *cobra.Command {
	return &cobra.Command{
		Use:   "resume <name>",
		Short: "Resume a deployment paused for authentication",
		Long: `Resume a deployment that is in the PausedAuth state.

The CLI's current authentication token is sent as the fresh credential;
the orchestration workflow restarts delivery with it.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := pb.NewDeploymentServiceClient(ctx.conn)

			dep, err := client.ResumeDeployment(cmd.Context(), &pb.ResumeDeploymentRequest{
				Name: qualifyDeploymentName(args[0]),
			})
			if err != nil {
				return err
			}

			return ctx.printer.PrintResource(dep, deploymentColumns())
		},
	}
}
