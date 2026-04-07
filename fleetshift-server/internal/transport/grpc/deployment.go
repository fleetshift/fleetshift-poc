package grpc

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const deploymentCollection = "deployments/"

// DeploymentServer implements [pb.DeploymentServiceServer].
type DeploymentServer struct {
	pb.UnimplementedDeploymentServiceServer
	Deployments *application.DeploymentService
}

func (s *DeploymentServer) CreateDeployment(ctx context.Context, req *pb.CreateDeploymentRequest) (*pb.Deployment, error) {
	if req.GetDeploymentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "deployment_id is required")
	}
	if req.GetDeployment() == nil {
		return nil, status.Error(codes.InvalidArgument, "deployment is required")
	}

	input, err := createInputFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid deployment: %v", err)
	}

	dep, err := s.Deployments.Create(ctx, input)
	if err != nil {
		return nil, domainError(err)
	}

	return deploymentToProto(dep), nil
}

func (s *DeploymentServer) GetDeployment(ctx context.Context, req *pb.GetDeploymentRequest) (*pb.Deployment, error) {
	id, err := parseDeploymentName(req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid name: %v", err)
	}

	dep, err := s.Deployments.Get(ctx, id)
	if err != nil {
		return nil, domainError(err)
	}

	return deploymentToProto(dep), nil
}

func (s *DeploymentServer) ResumeDeployment(ctx context.Context, req *pb.ResumeDeploymentRequest) (*pb.Deployment, error) {
	id, err := parseDeploymentName(req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid name: %v", err)
	}

	in := application.ResumeInput{
		ID:            id,
		UserSignature: req.GetUserSignature(),
	}
	if req.GetValidUntil() != nil {
		in.ValidUntil = req.GetValidUntil().AsTime()
	}

	dep, err := s.Deployments.Resume(ctx, in)
	if err != nil {
		return nil, domainError(err)
	}

	return deploymentToProto(dep), nil
}

func (s *DeploymentServer) ListDeployments(ctx context.Context, _ *pb.ListDeploymentsRequest) (*pb.ListDeploymentsResponse, error) {
	// TODO: implement pagination
	deps, err := s.Deployments.List(ctx)
	if err != nil {
		return nil, domainError(err)
	}

	out := make([]*pb.Deployment, len(deps))
	for i, d := range deps {
		out[i] = deploymentToProto(d)
	}
	return &pb.ListDeploymentsResponse{Deployments: out}, nil
}

// --- resource name helpers ---

func deploymentName(id domain.DeploymentID) string {
	return deploymentCollection + string(id)
}

func parseDeploymentName(name string) (domain.DeploymentID, error) {
	id, ok := strings.CutPrefix(name, deploymentCollection)
	if !ok || id == "" {
		return "", fmt.Errorf("name must have format %s{id}", deploymentCollection)
	}
	return domain.DeploymentID(id), nil
}

// --- proto <-> domain mapping ---

func createInputFromProto(req *pb.CreateDeploymentRequest) (domain.CreateDeploymentInput, error) {
	d := req.GetDeployment()
	ms, err := manifestStrategyFromProto(d.GetManifestStrategy())
	if err != nil {
		return domain.CreateDeploymentInput{}, err
	}
	ps, err := placementStrategyFromProto(d.GetPlacementStrategy())
	if err != nil {
		return domain.CreateDeploymentInput{}, err
	}
	var rs *domain.RolloutStrategySpec
	if d.GetRolloutStrategy() != nil {
		v, err := rolloutStrategyFromProto(d.GetRolloutStrategy())
		if err != nil {
			return domain.CreateDeploymentInput{}, err
		}
		rs = &v
	}
	in := domain.CreateDeploymentInput{
		ID:                domain.DeploymentID(req.GetDeploymentId()),
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		RolloutStrategy:   rs,
		UserSignature:     req.GetUserSignature(),
		ExpectedGeneration: domain.Generation(req.GetExpectedGeneration()),
	}
	if req.GetValidUntil() != nil {
		in.ValidUntil = req.GetValidUntil().AsTime()
	}
	return in, nil
}

func manifestStrategyFromProto(p *pb.ManifestStrategy) (domain.ManifestStrategySpec, error) {
	if p == nil {
		return domain.ManifestStrategySpec{}, fmt.Errorf("manifest_strategy is required")
	}
	switch p.GetType() {
	case pb.ManifestStrategy_TYPE_INLINE:
		manifests := make([]domain.Manifest, len(p.GetManifests()))
		for i, m := range p.GetManifests() {
			manifests[i] = domain.Manifest{
				ResourceType: domain.ResourceType(m.GetResourceType()),
				Raw:          m.GetRaw(),
			}
		}
		return domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: manifests,
		}, nil
	default:
		return domain.ManifestStrategySpec{}, fmt.Errorf("unsupported manifest_strategy type: %v", p.GetType())
	}
}

func placementStrategyFromProto(p *pb.PlacementStrategy) (domain.PlacementStrategySpec, error) {
	if p == nil {
		return domain.PlacementStrategySpec{}, fmt.Errorf("placement_strategy is required")
	}
	switch p.GetType() {
	case pb.PlacementStrategy_TYPE_STATIC:
		targets := make([]domain.TargetID, len(p.GetTargetIds()))
		for i, id := range p.GetTargetIds() {
			targets[i] = domain.TargetID(id)
		}
		return domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: targets,
		}, nil
	case pb.PlacementStrategy_TYPE_ALL:
		return domain.PlacementStrategySpec{Type: domain.PlacementStrategyAll}, nil
	case pb.PlacementStrategy_TYPE_SELECTOR:
		sel := p.GetTargetSelector()
		if sel == nil {
			return domain.PlacementStrategySpec{}, fmt.Errorf("target_selector is required for SELECTOR placement")
		}
		return domain.PlacementStrategySpec{
			Type:           domain.PlacementStrategySelector,
			TargetSelector: &domain.TargetSelector{MatchLabels: sel.GetMatchLabels()},
		}, nil
	default:
		return domain.PlacementStrategySpec{}, fmt.Errorf("unsupported placement_strategy type: %v", p.GetType())
	}
}

func rolloutStrategyFromProto(p *pb.RolloutStrategy) (domain.RolloutStrategySpec, error) {
	switch p.GetType() {
	case pb.RolloutStrategy_TYPE_IMMEDIATE:
		return domain.RolloutStrategySpec{Type: domain.RolloutStrategyImmediate}, nil
	default:
		return domain.RolloutStrategySpec{}, fmt.Errorf("unsupported rollout_strategy type: %v", p.GetType())
	}
}

func deploymentToProto(d domain.Deployment) *pb.Deployment {
	dep := &pb.Deployment{
		Name:  deploymentName(d.ID),
		State: deploymentStateToProto(d.State),
	}

	dep.Reconciling = dep.State == pb.Deployment_STATE_CREATING ||
		dep.State == pb.Deployment_STATE_DELETING ||
		dep.State == pb.Deployment_STATE_PAUSED_AUTH

	dep.ManifestStrategy = manifestStrategyToProto(d.ManifestStrategy)
	dep.PlacementStrategy = placementStrategyToProto(d.PlacementStrategy)
	if d.RolloutStrategy != nil {
		dep.RolloutStrategy = &pb.RolloutStrategy{
			Type: rolloutStrategyTypeToProto(d.RolloutStrategy.Type),
		}
	}

	if len(d.ResolvedTargets) > 0 {
		ids := make([]string, len(d.ResolvedTargets))
		for i, t := range d.ResolvedTargets {
			ids[i] = string(t)
		}
		dep.ResolvedTargetIds = ids
	}

	if !d.CreatedAt.IsZero() {
		dep.CreateTime = timestamppb.New(d.CreatedAt)
	}
	if !d.UpdatedAt.IsZero() {
		dep.UpdateTime = timestamppb.New(d.UpdatedAt)
	}
	dep.Uid = d.UID
	dep.Etag = d.Etag

	if d.Provenance != nil {
		dep.Provenance = provenanceToProto(d.Provenance)
	}

	return dep
}

func provenanceToProto(p *domain.Provenance) *pb.Provenance {
	prov := &pb.Provenance{
		Signature: &pb.Signature{
			Signer: &pb.FederatedIdentity{
				Subject: string(p.Sig.Signer.Subject),
				Issuer:  string(p.Sig.Signer.Issuer),
			},
			ContentHash:    p.Sig.ContentHash,
			SignatureBytes: p.Sig.SignatureBytes,
		},
		ValidUntil:         timestamppb.New(p.ValidUntil),
		ExpectedGeneration: int64(p.ExpectedGeneration),
	}

	if len(p.OutputConstraints) > 0 {
		prov.OutputConstraints = make([]*pb.OutputConstraint, len(p.OutputConstraints))
		for i, c := range p.OutputConstraints {
			prov.OutputConstraints[i] = &pb.OutputConstraint{
				Name:       c.Name,
				Expression: c.Expression,
			}
		}
	}

	return prov
}


func deploymentStateToProto(s domain.DeploymentState) pb.Deployment_State {
	switch s {
	case domain.DeploymentStateCreating:
		return pb.Deployment_STATE_CREATING
	case domain.DeploymentStateActive:
		return pb.Deployment_STATE_ACTIVE
	case domain.DeploymentStateDeleting:
		return pb.Deployment_STATE_DELETING
	case domain.DeploymentStateFailed:
		return pb.Deployment_STATE_FAILED
	case domain.DeploymentStatePausedAuth:
		return pb.Deployment_STATE_PAUSED_AUTH
	default:
		return pb.Deployment_STATE_UNSPECIFIED
	}
}

func manifestStrategyToProto(s domain.ManifestStrategySpec) *pb.ManifestStrategy {
	ms := &pb.ManifestStrategy{}
	switch s.Type {
	case domain.ManifestStrategyInline:
		ms.Type = pb.ManifestStrategy_TYPE_INLINE
	}
	if len(s.Manifests) > 0 {
		ms.Manifests = make([]*pb.Manifest, len(s.Manifests))
		for i, m := range s.Manifests {
			ms.Manifests[i] = &pb.Manifest{
				ResourceType: string(m.ResourceType),
				Raw:          m.Raw,
			}
		}
	}
	return ms
}

func placementStrategyToProto(s domain.PlacementStrategySpec) *pb.PlacementStrategy {
	ps := &pb.PlacementStrategy{}
	switch s.Type {
	case domain.PlacementStrategyStatic:
		ps.Type = pb.PlacementStrategy_TYPE_STATIC
		ids := make([]string, len(s.Targets))
		for i, t := range s.Targets {
			ids[i] = string(t)
		}
		ps.TargetIds = ids
	case domain.PlacementStrategyAll:
		ps.Type = pb.PlacementStrategy_TYPE_ALL
	case domain.PlacementStrategySelector:
		ps.Type = pb.PlacementStrategy_TYPE_SELECTOR
		if s.TargetSelector != nil {
			ps.TargetSelector = &pb.TargetSelector{MatchLabels: s.TargetSelector.MatchLabels}
		}
	}
	return ps
}

func rolloutStrategyTypeToProto(t domain.RolloutStrategyType) pb.RolloutStrategy_Type {
	switch t {
	case domain.RolloutStrategyImmediate:
		return pb.RolloutStrategy_TYPE_IMMEDIATE
	default:
		return pb.RolloutStrategy_TYPE_UNSPECIFIED
	}
}

// --- error mapping ---

func domainError(err error) error {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, domain.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
