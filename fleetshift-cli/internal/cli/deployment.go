package cli

import (
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/output"
	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/spf13/cobra"
)

const deploymentCollection = "deployments/"

func newDeploymentCmd(ctx *cmdContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "deployment",
		Aliases: []string{"dep", "deployments"},
		Short:   "Manage deployments",
	}

	cmd.AddCommand(
		newDeploymentCreateCmd(ctx),
		newDeploymentGetCmd(ctx),
		newDeploymentListCmd(ctx),
		newDeploymentResumeCmd(ctx),
		newDeploymentDeleteCmd(ctx),
	)

	return cmd
}

func deploymentColumns() []output.Column {
	return []output.Column{
		{Header: "Name", Value: func(m proto.Message) string {
			return m.(*pb.Deployment).GetName()
		}},
		{Header: "State", Value: func(m proto.Message) string {
			return formatState(m.(*pb.Deployment).GetState())
		}},
		{Header: "Reconciling", Value: func(m proto.Message) string {
			if m.(*pb.Deployment).GetReconciling() {
				return "true"
			}
			return "false"
		}},
		{Header: "Targets", Value: func(m proto.Message) string {
			ids := m.(*pb.Deployment).GetResolvedTargetIds()
			if len(ids) == 0 {
				return "-"
			}
			return strings.Join(ids, ",")
		}},
		{Header: "Age", Value: func(m proto.Message) string {
			t := m.(*pb.Deployment).GetCreateTime()
			if t == nil || !t.IsValid() {
				return "-"
			}
			return formatAge(t.AsTime())
		}},
	}
}

func formatState(s pb.Deployment_State) string {
	switch s {
	case pb.Deployment_STATE_CREATING:
		return "Creating"
	case pb.Deployment_STATE_ACTIVE:
		return "Active"
	case pb.Deployment_STATE_DELETING:
		return "Deleting"
	case pb.Deployment_STATE_FAILED:
		return "Failed"
	case pb.Deployment_STATE_PAUSED_AUTH:
		return "PausedAuth"
	default:
		return "Unknown"
	}
}

func qualifyDeploymentName(name string) string {
	if strings.HasPrefix(name, deploymentCollection) {
		return name
	}
	return deploymentCollection + name
}
