package cli

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/pkg/canonical"
	"github.com/spf13/cobra"
)

type createDeploymentFlags struct {
	id             string
	manifestFile   string
	resourceType   string
	placementType  string
	targetIDs      []string
	targetSelector map[string]string
	rolloutType    string
	sign           bool
}

func newDeploymentCreateCmd(ctx *cmdContext) *cobra.Command {
	f := &createDeploymentFlags{}

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a deployment",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			req, err := buildCreateRequest(f)
			if err != nil {
				return err
			}

			if f.sign {
				if err := signCreateRequest(req); err != nil {
					return fmt.Errorf("sign deployment: %w", err)
				}
			}

			client := pb.NewDeploymentServiceClient(ctx.conn)
			dep, err := client.CreateDeployment(cmd.Context(), req)
			if err != nil {
				return err
			}

			return ctx.printer.PrintResource(dep, deploymentColumns())
		},
	}

	cmd.Flags().StringVar(&f.id, "id", "", "deployment identifier (required)")
	cmd.Flags().StringVar(&f.manifestFile, "manifest-file", "", "path to manifest JSON file (use - for stdin)")
	cmd.Flags().StringVar(&f.resourceType, "resource-type", "", "manifest resource type (e.g. api.kind.cluster)")
	cmd.Flags().StringVar(&f.placementType, "placement-type", "all", "placement strategy: static, all, selector")
	cmd.Flags().StringSliceVar(&f.targetIDs, "target-ids", nil, "target IDs for static placement (comma-separated)")
	cmd.Flags().StringToStringVar(&f.targetSelector, "target-selector", nil, "label selector for selector placement (key=val,...)")
	cmd.Flags().StringVar(&f.rolloutType, "rollout-type", "immediate", "rollout strategy: immediate")
	cmd.Flags().BoolVar(&f.sign, "sign", false, "Sign the deployment intent with the enrolled signing key")

	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("manifest-file")
	_ = cmd.MarkFlagRequired("resource-type")

	return cmd
}

func buildCreateRequest(f *createDeploymentFlags) (*pb.CreateDeploymentRequest, error) {
	manifest, err := readManifest(f.manifestFile)
	if err != nil {
		return nil, err
	}

	ms := &pb.ManifestStrategy{
		Type: pb.ManifestStrategy_TYPE_INLINE,
		Manifests: []*pb.Manifest{{
			ResourceType: f.resourceType,
			Raw:          manifest,
		}},
	}

	ps, err := buildPlacementStrategy(f)
	if err != nil {
		return nil, err
	}

	rs, err := buildRolloutStrategy(f.rolloutType)
	if err != nil {
		return nil, err
	}

	return &pb.CreateDeploymentRequest{
		DeploymentId: f.id,
		Deployment: &pb.Deployment{
			ManifestStrategy:  ms,
			PlacementStrategy: ps,
			RolloutStrategy:   rs,
		},
	}, nil
}

func readManifest(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

func buildPlacementStrategy(f *createDeploymentFlags) (*pb.PlacementStrategy, error) {
	switch strings.ToLower(f.placementType) {
	case "all":
		return &pb.PlacementStrategy{Type: pb.PlacementStrategy_TYPE_ALL}, nil
	case "static":
		if len(f.targetIDs) == 0 {
			return nil, fmt.Errorf("--target-ids is required for static placement")
		}
		return &pb.PlacementStrategy{
			Type:      pb.PlacementStrategy_TYPE_STATIC,
			TargetIds: f.targetIDs,
		}, nil
	case "selector":
		if len(f.targetSelector) == 0 {
			return nil, fmt.Errorf("--target-selector is required for selector placement")
		}
		return &pb.PlacementStrategy{
			Type:           pb.PlacementStrategy_TYPE_SELECTOR,
			TargetSelector: &pb.TargetSelector{MatchLabels: f.targetSelector},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported placement type %q (valid: all, static, selector)", f.placementType)
	}
}

func buildRolloutStrategy(rolloutType string) (*pb.RolloutStrategy, error) {
	switch strings.ToLower(rolloutType) {
	case "immediate":
		return &pb.RolloutStrategy{Type: pb.RolloutStrategy_TYPE_IMMEDIATE}, nil
	default:
		return nil, fmt.Errorf("unsupported rollout type %q (valid: immediate)", rolloutType)
	}
}

// signCreateRequest loads the signing key, builds the canonical envelope,
// signs it, and populates the signing fields on the request.
func signCreateRequest(req *pb.CreateDeploymentRequest) error {
	privKey, err := loadSigningPrivateKey()
	if err != nil {
		return err
	}

	ms, ps := canonicalStrategiesFromProto(req.GetDeployment())

	validUntil := time.Now().Add(24 * time.Hour)
	var expectedGeneration int64 = 1

	envelopeBytes, err := canonical.BuildSignedInputEnvelope(
		req.GetDeploymentId(),
		ms, ps, validUntil, nil, expectedGeneration,
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
	req.ExpectedGeneration = expectedGeneration
	return nil
}

func loadSigningPrivateKey() (*ecdsa.PrivateKey, error) {
	pemStr, err := auth.LoadSigningKey()
	if err != nil {
		return nil, fmt.Errorf("load signing key: %w", err)
	}
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, fmt.Errorf("invalid PEM data for signing key")
	}
	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse signing key: %w", err)
	}
	return key, nil
}

func canonicalStrategiesFromProto(dep *pb.Deployment) (canonical.ManifestStrategy, canonical.PlacementStrategy) {
	var ms canonical.ManifestStrategy
	if p := dep.GetManifestStrategy(); p != nil {
		switch p.GetType() {
		case pb.ManifestStrategy_TYPE_INLINE:
			ms.Type = "inline"
			for _, m := range p.GetManifests() {
				ms.Manifests = append(ms.Manifests, canonical.Manifest{
					ResourceType: m.GetResourceType(),
					Raw:          m.GetRaw(),
				})
			}
		}
	}

	var ps canonical.PlacementStrategy
	if p := dep.GetPlacementStrategy(); p != nil {
		switch p.GetType() {
		case pb.PlacementStrategy_TYPE_STATIC:
			ps.Type = "static"
			ps.Targets = p.GetTargetIds()
		case pb.PlacementStrategy_TYPE_ALL:
			ps.Type = "all"
		case pb.PlacementStrategy_TYPE_SELECTOR:
			ps.Type = "selector"
			if sel := p.GetTargetSelector(); sel != nil {
				ps.MatchLabels = sel.GetMatchLabels()
			}
		}
	}

	return ms, ps
}
