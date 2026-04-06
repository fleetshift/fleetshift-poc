package cli

import (
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/pkg/canonical"
	"github.com/spf13/cobra"
)

func newDeploymentResumeCmd(ctx *cmdContext) *cobra.Command {
	var sign bool

	cmd := &cobra.Command{
		Use:   "resume <name>",
		Short: "Resume a deployment paused for authentication",
		Long: `Resume a deployment that is in the PausedAuth state.

The CLI's current authentication token is sent as the fresh credential;
the orchestration workflow restarts delivery with it.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := pb.NewDeploymentServiceClient(ctx.conn)
			name := qualifyDeploymentName(args[0])

			req := &pb.ResumeDeploymentRequest{
				Name: name,
			}

			if sign {
				if err := signResumeRequest(cmd, client, req); err != nil {
					return fmt.Errorf("sign resume: %w", err)
				}
			}

			dep, err := client.ResumeDeployment(cmd.Context(), req)
			if err != nil {
				return err
			}

			return ctx.printer.PrintResource(dep, deploymentColumns())
		},
	}

	cmd.Flags().BoolVar(&sign, "sign", false, "Re-sign the deployment intent with the enrolled signing key")

	return cmd
}

func signResumeRequest(cmd *cobra.Command, client pb.DeploymentServiceClient, req *pb.ResumeDeploymentRequest) error {
	dep, err := client.GetDeployment(cmd.Context(), &pb.GetDeploymentRequest{
		Name: req.Name,
	})
	if err != nil {
		return fmt.Errorf("get deployment: %w", err)
	}

	privKey, err := loadSigningPrivateKey()
	if err != nil {
		return err
	}

	ms, ps := canonicalStrategiesFromProto(dep)

	depID := dep.GetName()
	if idx := len("deployments/"); len(depID) > idx {
		depID = depID[idx:]
	}

	var expectedGeneration int64
	if dep.GetProvenance() != nil {
		expectedGeneration = dep.GetProvenance().GetExpectedGeneration() + 1
	}

	validUntil := time.Now().Add(24 * time.Hour)

	envelopeBytes, err := canonical.BuildSignedInputEnvelope(
		depID, ms, ps, validUntil, nil, expectedGeneration,
	)
	if err != nil {
		return fmt.Errorf("build signed input envelope: %w", err)
	}

	envelopeHash := canonical.HashIntent(envelopeBytes)
	sig, err := ecdsa.SignASN1(rand.Reader, privKey, envelopeHash)
	if err != nil {
		return fmt.Errorf("sign intent: %w", err)
	}

	req.UserSignature = sig
	req.ValidUntil = timestamppb.New(validUntil)
	return nil
}
