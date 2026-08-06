package cli

import (
	"google.golang.org/protobuf/proto"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/spf13/cobra"
)

type listDeploymentFlags struct {
	pageSize int32
}

func newDeploymentListCmd(ctx *cmdContext) *cobra.Command {
	f := &listDeploymentFlags{}

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List deployments",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := pb.NewDeploymentServiceClient(ctx.conn)

			resp, err := client.ListDeployments(cmd.Context(), &pb.ListDeploymentsRequest{
				PageSize: f.pageSize,
			})
			if err != nil {
				return err
			}

			msgs := make([]proto.Message, len(resp.GetDeployments()))
			for i, d := range resp.GetDeployments() {
				msgs[i] = d
			}

			return ctx.printer.PrintResourceList(msgs, deploymentColumns())
		},
	}

	cmd.Flags().Int32Var(&f.pageSize, "page-size", 0, "maximum number of deployments to return (0 = server default)")

	return cmd
}
