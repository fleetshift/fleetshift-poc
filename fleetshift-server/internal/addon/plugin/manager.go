package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"sync"

	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"

	monpb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/addon/monitoring/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

var handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "FLEETSHIFT_MONITORING",
	MagicCookieValue: "v1",
}

type monitoringGRPCPlugin struct {
	plugin.Plugin
}

func (p *monitoringGRPCPlugin) GRPCServer(_ *plugin.GRPCBroker, _ *grpc.Server) error {
	return fmt.Errorf("server-side not implemented in host")
}

func (p *monitoringGRPCPlugin) GRPCClient(_ context.Context, _ *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return monpb.NewMonitoringAddonClient(c), nil
}

// Manager launches and manages go-plugin addon subprocesses.
type Manager struct {
	logger *slog.Logger

	mu      sync.Mutex
	clients map[string]*plugin.Client
}

func NewManager(logger *slog.Logger) *Manager {
	return &Manager{
		logger:  logger.With("component", "addon-plugin-manager"),
		clients: make(map[string]*plugin.Client),
	}
}

func (m *Manager) GenerateManifests(ctx context.Context, addonName string, targetID domain.TargetID) ([]domain.Manifest, error) {
	client, err := m.getOrLaunch(addonName)
	if err != nil {
		return nil, fmt.Errorf("launch addon %q: %w", addonName, err)
	}

	rpcClient, err := client.Client()
	if err != nil {
		return nil, fmt.Errorf("get rpc client for %q: %w", addonName, err)
	}

	raw, err := rpcClient.Dispense("monitoring")
	if err != nil {
		return nil, fmt.Errorf("dispense %q: %w", addonName, err)
	}

	monClient := raw.(monpb.MonitoringAddonClient)
	resp, err := monClient.GenerateManifests(ctx, &monpb.GenerateManifestsRequest{
		TargetId: string(targetID),
	})
	if err != nil {
		return nil, fmt.Errorf("generate manifests from %q: %w", addonName, err)
	}

	manifests := make([]domain.Manifest, len(resp.Manifests))
	for i, raw := range resp.Manifests {
		manifests[i] = domain.Manifest{
			ResourceType: domain.ResourceType("addon." + addonName),
			Raw:          json.RawMessage(raw),
		}
	}

	m.logger.Info("addon generated manifests",
		"addon", addonName,
		"target_id", targetID,
		"count", len(manifests))

	return manifests, nil
}

func (m *Manager) getOrLaunch(addonName string) (*plugin.Client, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if c, ok := m.clients[addonName]; ok && !c.Exited() {
		return c, nil
	}

	binaryPath, err := exec.LookPath(addonName)
	if err != nil {
		return nil, fmt.Errorf("addon binary %q not found in PATH: %w", addonName, err)
	}

	m.logger.Info("launching addon plugin", "addon", addonName, "binary", binaryPath)

	c := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: handshake,
		Plugins: map[string]plugin.Plugin{
			"monitoring": &monitoringGRPCPlugin{},
		},
		Cmd:              exec.Command(binaryPath),
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
	})

	m.clients[addonName] = c
	return c, nil
}

func (m *Manager) Kill() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, c := range m.clients {
		m.logger.Info("killing addon plugin", "addon", name)
		c.Kill()
	}
}
