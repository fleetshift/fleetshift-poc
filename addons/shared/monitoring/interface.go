package monitoring

import (
	"context"

	"google.golang.org/grpc"

	"github.com/hashicorp/go-plugin"

	pb "github.com/fleetshift/fleetshift-poc/addons/gen/addon/monitoring/v1"
)

var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "FLEETSHIFT_MONITORING",
	MagicCookieValue: "v1",
}

var PluginMap = map[string]plugin.Plugin{
	"monitoring": &MonitoringGRPCPlugin{},
}

type MonitoringAddon interface {
	Collect(ctx context.Context) (*pb.CollectResponse, error)
}

type MonitoringGRPCPlugin struct {
	plugin.Plugin
	Impl MonitoringAddon
}

func (p *MonitoringGRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	pb.RegisterMonitoringAddonServer(s, &GRPCServer{Impl: p.Impl})
	return nil
}

func (p *MonitoringGRPCPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &grpcClient{client: pb.NewMonitoringAddonClient(c)}, nil
}
