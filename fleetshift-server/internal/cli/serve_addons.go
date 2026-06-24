package cli

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"sigs.k8s.io/kind/pkg/cluster"
	kindlog "sigs.k8s.io/kind/pkg/log"

	gcphcpaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	kubernetesaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// addonState holds the constructed addon instances and configuration
// produced during startup, threaded through the lifecycle phases.
type addonState struct {
	kindAgent           domain.DeliveryAgent
	gcphcpAgent         domain.DeliveryAgent
	gcphcpConcreteAgent *gcphcpaddon.Agent
	gcphcpCfg           gcphcpaddon.Config
	k8sPool             *kubernetesaddon.AgentPool
}

// constructAddons builds the delivery agents for each enabled addon.
// Agent construction is separate from addon Enable/Connect because agents
// have external dependencies (Docker, AWS creds, etc.) that the addon
// manager should not own.
func constructAddons(
	ctx context.Context,
	enabledAddons map[string]bool,
	f *serveFlags,
	logger *slog.Logger,
	deliveryReporter domain.DeliveryReporter,
	store domain.Store,
	vault domain.Vault,
	keyResolver *domain.KeyResolver,
	oidcHTTPClient *http.Client,
	oidcCABundle []byte,
) (*addonState, error) {
	agents := &addonState{}

	if enabledAddons["kind"] {
		kindOpts := []kindaddon.AgentOption{
			kindaddon.WithObserver(kindaddon.NewSlogAgentObserver(logger)),
		}
		if tempDir := os.Getenv("KIND_TEMP_DIR"); tempDir != "" {
			kindOpts = append(kindOpts, kindaddon.WithTempDir(tempDir))
			logger.Info("kind agent: using temp dir " + tempDir)
		}
		if oidcCABundle != nil {
			kindOpts = append(kindOpts, kindaddon.WithOIDCCABundle(oidcCABundle))
		}
		if containerHost := os.Getenv("CONTAINER_HOST"); containerHost != "" {
			kindOpts = append(kindOpts, kindaddon.WithContainerHost(containerHost))
			logger.Info("kind agent: rewriting localhost OIDC issuer URLs to " + containerHost)
		}
		if httpsPort := os.Getenv("OIDC_HTTPS_PORT"); httpsPort != "" {
			kindOpts = append(kindOpts, kindaddon.WithOIDCHTTPSPort(httpsPort))
			logger.Info("kind agent: upgrading HTTP OIDC issuer URLs to HTTPS on port " + httpsPort)
		}
		agents.kindAgent = kindaddon.NewAgent(
			deliveryReporter,
			func(logger kindlog.Logger) kindaddon.ClusterProvider {
				var opts []cluster.ProviderOption
				if logger != nil {
					opts = append(opts, cluster.ProviderWithLogger(logger))
				}
				return cluster.NewProvider(opts...)
			},
			kindOpts...,
		)
	}

	if enabledAddons["gcphcp"] {
		configPath := f.gcphcpConfig
		if configPath == "" {
			configPath = os.Getenv("GCPHCP_CONFIG")
		}
		if configPath != "" {
			var err error
			agents.gcphcpCfg, err = gcphcpaddon.ParseConfig(configPath)
			if err != nil {
				return nil, fmt.Errorf("parse gcphcp config: %w", err)
			}
			agents.gcphcpConcreteAgent = gcphcpaddon.NewAgent(gcphcpaddon.AgentDeps{
				Gateway:  agents.gcphcpCfg.Gateway,
				Observer: gcphcpaddon.NewSlogAgentObserver(logger),
				Reporter: deliveryReporter,
			})
			agents.gcphcpAgent = agents.gcphcpConcreteAgent
		} else {
			logger.Warn("gcphcp addon enabled but no config provided, skipping")
			delete(enabledAddons, "gcphcp")
		}
	}

	if enabledAddons["kubernetes"] {
		inventoryWriter := application.NewInventoryWriteService(store)
		agents.k8sPool = kubernetesaddon.NewAgentPool(ctx, store, vault, inventoryWriter, deliveryReporter, keyResolver, oidcHTTPClient, logger.With("component", "kubernetes-agent"))
	}

	return agents, nil
}

// enableAddons transitions each enabled addon from Defined to Enabled,
// authorizing the addon and recording capability expectations.
func enableAddons(ctx context.Context, addonMgr *application.AddonManager, enabledAddons map[string]bool) error {
	if enabledAddons["kind"] {
		if err := addonMgr.Enable(ctx, kindaddon.Descriptor()); err != nil {
			return fmt.Errorf("enable kind addon: %w", err)
		}
	}
	if enabledAddons["gcphcp"] {
		if err := addonMgr.Enable(ctx, gcphcpaddon.Descriptor()); err != nil {
			return fmt.Errorf("enable gcphcp addon: %w", err)
		}
	}
	if enabledAddons["kubernetes"] {
		if err := addonMgr.Enable(ctx, kubernetesaddon.Descriptor()); err != nil {
			return fmt.Errorf("enable kubernetes addon: %w", err)
		}
	}
	return nil
}

// connectAddons transitions each enabled addon from Enabled to Connected,
// registering delivery agents, compiling schemas, and seeding targets.
func connectAddons(ctx context.Context, addonMgr *application.AddonManager, enabledAddons map[string]bool, agents *addonState, logger *slog.Logger) error {
	if enabledAddons["kind"] {
		if err := addonMgr.Connect(ctx, "kind", application.ConnectInput{
			DeliveryAgent: agents.kindAgent,
			Targets: []domain.TargetInfo{domain.NewTargetInfo(
				"kind-local",
				kindaddon.TargetType,
				"Local Kind Provider",
				domain.TargetStateReady,
				nil,
				nil,
				[]domain.ResourceType{kindaddon.ClusterResourceType, domain.TrustBundleResourceType},
			)},
			Schemas: []domain.ManagedResourceSchema{kindaddon.Schema()},
		}); err != nil {
			return fmt.Errorf("connect kind addon: %w", err)
		}
	}

	if enabledAddons["kubernetes"] {
		if err := addonMgr.Connect(ctx, "kubernetes", application.ConnectInput{
			DeliveryAgent: agents.k8sPool,
			IndexAgent:    agents.k8sPool,
		}); err != nil {
			return fmt.Errorf("connect kubernetes addon: %w", err)
		}
	}

	if enabledAddons["gcphcp"] {
		activeTarget := agents.gcphcpCfg.Targets[0]
		targetID := domain.TargetID(activeTarget.ID)
		if err := addonMgr.Connect(ctx, "gcphcp", application.ConnectInput{
			DeliveryAgent: agents.gcphcpAgent,
			Targets: []domain.TargetInfo{domain.NewTargetInfo(
				targetID,
				gcphcpaddon.TargetType,
				fmt.Sprintf("GCP HCP %s/%s", activeTarget.GCPProject, activeTarget.Region),
				domain.TargetStateReady,
				nil,
				activeTarget.TargetProperties(),
				[]domain.ResourceType{gcphcpaddon.ClusterResourceType, domain.TrustBundleResourceType},
			)},
			Schemas: []domain.ManagedResourceSchema{gcphcpaddon.Schema(targetID)},
		}); err != nil {
			return fmt.Errorf("connect gcphcp addon: %w", err)
		}
		if agents.gcphcpConcreteAgent != nil {
			if err := agents.gcphcpConcreteAgent.RecoverActiveDeliveries(ctx, []domain.TargetID{targetID}); err != nil {
				logger.Error("gcphcp: failed to recover active deliveries", "error", err)
			}
		}
	}

	return nil
}

// shutdownAddons performs graceful shutdown of addon agents.
func shutdownAddons(agents *addonState) {
	if agents.k8sPool != nil {
		agents.k8sPool.StopAll()
	}
}

// parseAddons splits a comma-separated addon list into a set.
func parseAddons(spec string) map[string]bool {
	addons := make(map[string]bool)
	if spec == "" {
		return addons
	}
	for _, a := range strings.Split(spec, ",") {
		a = strings.TrimSpace(a)
		if a != "" {
			addons[a] = true
		}
	}
	return addons
}

// buildTrustBundlePlacement returns a static placement targeting every
// addon that consumes trust bundles.
func buildTrustBundlePlacement(enabledAddons map[string]bool, gcphcpTargetID string) domain.PlacementStrategySpec {
	targets := make([]domain.TargetID, 0, 2)
	if enabledAddons["kind"] {
		targets = append(targets, "kind-local")
	}
	if enabledAddons["gcphcp"] && gcphcpTargetID != "" {
		targets = append(targets, domain.TargetID(gcphcpTargetID))
	}
	if len(targets) == 0 {
		return domain.PlacementStrategySpec{}
	}
	return domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: targets,
	}
}
