package monitoring

import (
	"context"

	pb "github.com/fleetshift/fleetshift-poc/addons/gen/addon/monitoring/v1"
)

type grpcClient struct {
	client pb.MonitoringAddonClient
}

func (c *grpcClient) Collect(ctx context.Context) (*pb.CollectResponse, error) {
	return c.client.Collect(ctx, &pb.CollectRequest{})
}

func (c *grpcClient) GenerateManifests(ctx context.Context, targetID string) ([][]byte, error) {
	resp, err := c.client.GenerateManifests(ctx, &pb.GenerateManifestsRequest{TargetId: targetID})
	if err != nil {
		return nil, err
	}
	return resp.Manifests, nil
}

type GRPCServer struct {
	pb.UnimplementedMonitoringAddonServer
	Impl MonitoringAddon
}

func (s *GRPCServer) Collect(ctx context.Context, req *pb.CollectRequest) (*pb.CollectResponse, error) {
	return s.Impl.Collect(ctx)
}

func (s *GRPCServer) GenerateManifests(ctx context.Context, req *pb.GenerateManifestsRequest) (*pb.GenerateManifestsResponse, error) {
	manifests, err := s.Impl.GenerateManifests(ctx, req.TargetId)
	if err != nil {
		return nil, err
	}
	return &pb.GenerateManifestsResponse{Manifests: manifests}, nil
}
