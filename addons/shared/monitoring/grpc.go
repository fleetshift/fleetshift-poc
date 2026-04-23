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

type GRPCServer struct {
	pb.UnimplementedMonitoringAddonServer
	Impl MonitoringAddon
}

func (s *GRPCServer) Collect(ctx context.Context, req *pb.CollectRequest) (*pb.CollectResponse, error) {
	return s.Impl.Collect(ctx)
}
