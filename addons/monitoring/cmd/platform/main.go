package main

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/hashicorp/go-plugin"

	pb "github.com/fleetshift/fleetshift-poc/addons/gen/addon/monitoring/v1"
	monitoringplugin "github.com/fleetshift/fleetshift-poc/addons/shared/monitoring"
	"github.com/fleetshift/fleetshift-poc/addons/monitoring/internal/manifests"
)

type PlatformAddon struct{}

func (a *PlatformAddon) Collect(_ context.Context) (*pb.CollectResponse, error) {
	return nil, fmt.Errorf("Collect is not supported on the platform side")
}

func (a *PlatformAddon) GenerateManifests(_ context.Context, targetID string) ([][]byte, error) {
	raw, err := manifests.GenerateMonitoringConfig("default", "30s", true, true)
	if err != nil {
		return nil, err
	}
	return [][]byte{raw}, nil
}

type PlatformAddonPlugin struct {
	plugin.Plugin
	Impl monitoringplugin.MonitoringAddon
}

func (p *PlatformAddonPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	pb.RegisterMonitoringAddonServer(s, &monitoringplugin.GRPCServer{Impl: p.Impl})
	return nil
}

func (p *PlatformAddonPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return nil, fmt.Errorf("platform addon is server-only")
}

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: monitoringplugin.Handshake,
		Plugins: map[string]plugin.Plugin{
			"monitoring": &PlatformAddonPlugin{Impl: &PlatformAddon{}},
		},
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
