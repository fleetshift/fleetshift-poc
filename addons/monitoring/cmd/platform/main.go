package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"google.golang.org/grpc"

	"github.com/hashicorp/go-plugin"

	pb "github.com/fleetshift/fleetshift-poc/addons/gen/addon/monitoring/v1"
	"github.com/fleetshift/fleetshift-poc/addons/monitoring/internal/manifests"
)

type PlatformAddon struct{}

func (a *PlatformAddon) Collect(ctx context.Context) (*pb.CollectResponse, error) {
	return nil, fmt.Errorf("Collect is not supported on the platform side")
}

func (a *PlatformAddon) GenerateManifests() ([][]byte, error) {
	raw, err := manifests.GenerateMonitoringConfig("default", "30s", true, true)
	if err != nil {
		return nil, err
	}
	return [][]byte{raw}, nil
}

func (a *PlatformAddon) HandleMetricsReport(data json.RawMessage) {
	log.Printf("received metrics report: %s", string(data))
}

var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "FLEETSHIFT_MONITORING_PLATFORM",
	MagicCookieValue: "v1",
}

type PlatformAddonPlugin struct {
	plugin.Plugin
	Impl *PlatformAddon
}

func (p *PlatformAddonPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	pb.RegisterMonitoringAddonServer(s, &platformGRPCServer{impl: p.Impl})
	return nil
}

func (p *PlatformAddonPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return nil, fmt.Errorf("platform addon is server-only")
}

type platformGRPCServer struct {
	pb.UnimplementedMonitoringAddonServer
	impl *PlatformAddon
}

func (s *platformGRPCServer) Collect(ctx context.Context, req *pb.CollectRequest) (*pb.CollectResponse, error) {
	return s.impl.Collect(ctx)
}

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: Handshake,
		Plugins: map[string]plugin.Plugin{
			"monitoring-platform": &PlatformAddonPlugin{Impl: &PlatformAddon{}},
		},
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
