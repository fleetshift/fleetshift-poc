package bootstrap

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"sigs.k8s.io/kind/pkg/cluster"
	kindlog "sigs.k8s.io/kind/pkg/log"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	kubernetesaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/scripted"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
)

// buildTrustBundlePlacement returns a static placement strategy for trust-bundle
// delivery when the Kind consumer is enabled.
func buildTrustBundlePlacement(enabledAddons map[AddonName]bool) domain.PlacementStrategySpec {
	targets := make([]domain.TargetID, 0, 1)
	if enabledAddons[AddonKind] {
		targets = append(targets, "kind-local")
	}
	if len(targets) == 0 {
		return domain.PlacementStrategySpec{}
	}
	return domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: targets,
	}
}

// assembleProductionAddons builds production Kind, Kubernetes, and scripted specs.
// Agent construction stays at the composition edge because agents have
// external dependencies (container runtimes, credentials, etc.) that AddonManager
// should not own; Enable/Connect only register capabilities and wire
// schemas/targets/agents into the running graph.
func assembleProductionAddons(
	deps AddonDeps,
	keyResolver *domain.KeyResolver,
	oidcHTTPClient *http.Client,
) ([]AddonSpec, error) {
	enabled := deps.Config.AddonSet()
	var specs []AddonSpec

	if enabled[AddonKind] {
		kindOpts := []kindaddon.AgentOption{
			kindaddon.WithObserver(kindaddon.NewSlogAgentObserver(deps.Logger)),
			kindaddon.WithInventoryWatcher(kindaddon.NewInventoryWatcher(deps.InventoryReporter)),
		}
		if len(deps.Config.OIDCCABundle) > 0 {
			kindOpts = append(kindOpts, kindaddon.WithOIDCCABundle(deps.Config.OIDCCABundle))
		}
		if deps.Indexing != nil {
			kindOpts = append(kindOpts, kindaddon.WithIndexingRuntime(deps.Indexing.Runtime))
		}
		if destination := strings.TrimSpace(os.Getenv(kindaddon.LoopbackForwardToEnv)); destination != "" {
			fwd, err := kindaddon.NewLoopbackForward(destination)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", kindaddon.LoopbackForwardToEnv, err)
			}
			kindOpts = append(kindOpts, kindaddon.WithLoopbackForward(fwd))
		}
		kindAgent := kindaddon.NewAgent(
			deps.DeliveryReporter,
			func(logger kindlog.Logger) kindaddon.ClusterProvider {
				var opts []cluster.ProviderOption
				if logger != nil {
					opts = append(opts, cluster.ProviderWithLogger(logger))
				}
				return cluster.NewProvider(opts...)
			},
			kindOpts...,
		)
		if kindAgent == nil {
			return nil, fmt.Errorf("kind delivery agent is nil")
		}
		specs = append(specs, AddonSpec{
			Descriptor: kindaddon.Descriptor(),
			Connect: application.ConnectInput{
				Agent: kindAgent,
				Targets: []domain.TargetInfo{domain.NewTargetInfo(
					"kind-local",
					kindaddon.TargetType,
					"Local Kind Provider",
					domain.TargetStateReady,
					nil,
					nil,
					[]domain.ManifestType{kindaddon.ClusterManifestType, kindaddon.ManagedClusterManifestType, domain.TrustBundleManifestType},
				)},
				Schemas: []domain.ExtensionResourceSchema{kindaddon.Schema(), kindaddon.NodeSchema()},
			},
		})
	}

	if enabled[AddonKubernetes] {
		kubeAgentOpts := []kubernetesaddon.DeliveryAgentOption{
			kubernetesaddon.WithKeyResolver(keyResolver),
			kubernetesaddon.WithVault(deps.Vault),
		}
		if oidcHTTPClient != nil {
			kubeAgentOpts = append(kubeAgentOpts, kubernetesaddon.WithHTTPClient(oidcHTTPClient))
		}
		kubeAgent := kubernetesaddon.NewDeliveryAgent(deps.DeliveryReporter, kubeAgentOpts...)
		if kubeAgent == nil {
			return nil, fmt.Errorf("kubernetes delivery agent is nil")
		}
		specs = append(specs, AddonSpec{
			Descriptor: kubernetesaddon.Descriptor(),
			Connect: application.ConnectInput{
				Agent:   kubeAgent,
				Schemas: []domain.ExtensionResourceSchema{kubernetesaddon.InventorySchema()},
			},
		})
	}

	if enabled[AddonScripted] {
		codec, err := scripted.NewCodec(deps.AppCtx)
		if err != nil {
			return nil, fmt.Errorf("scripted addon codec: %w", err)
		}
		planner := scripted.NewPlanner()
		scriptedAgent := scripted.NewAgent(
			deps.DeliveryReporter,
			deps.InventoryReporter,
			codec,
			planner,
			deps.AppCtx,
			deps.Logger.With("addon", "scripted"),
		)
		specs = append(specs, AddonSpec{
			Descriptor: scripted.Descriptor(),
			Connect: application.ConnectInput{
				Agent: scriptedAgent,
				Targets: []domain.TargetInfo{domain.NewTargetInfo(
					scripted.TargetID,
					scripted.TargetType,
					"Local Scripted Provider",
					domain.TargetStateReady,
					nil,
					nil,
					[]domain.ManifestType{scripted.ManagedManifestType},
				)},
				Schemas: []domain.ExtensionResourceSchema{scripted.Schema()},
			},
			Close: scriptedAgent.Close,
		})
	}

	return specs, nil
}

// enableAndConnectAddons runs Enable/Connect and post-connect hooks for each
// spec. A claimed DeliveryCapability requires a non-nil Connect.Agent.
// It returns close hooks in reverse registration order for shutdown.
func enableAndConnectAddons(ctx context.Context, addonMgr *application.AddonManager, specs []AddonSpec, logger *slog.Logger) ([]func(context.Context) error, error) {
	var closeHooks []func(context.Context) error

	// reverseHooks returns the accumulated hooks in reverse order so
	// the caller can unwind partially-connected addons on failure.
	reverseHooks := func() []func(context.Context) error {
		out := make([]func(context.Context) error, len(closeHooks))
		copy(out, closeHooks)
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
		return out
	}

	for _, spec := range specs {
		if err := addonMgr.Enable(ctx, spec.Descriptor); err != nil {
			return reverseHooks(), fmt.Errorf("enable %s addon: %w", spec.Descriptor.ID, err)
		}
		if err := rejectNilClaimedAgent(spec); err != nil {
			return reverseHooks(), err
		}
		if err := addonMgr.Connect(ctx, spec.Descriptor.ID, spec.Connect); err != nil {
			return reverseHooks(), fmt.Errorf("connect %s addon: %w", spec.Descriptor.ID, err)
		}
		if spec.Close != nil {
			closeHooks = append(closeHooks, spec.Close)
		}
		if spec.AfterConnect != nil {
			if err := spec.AfterConnect(ctx); err != nil {
				return reverseHooks(), err
			}
		}
		if spec.AfterConnectBestEffort != nil {
			if err := spec.AfterConnectBestEffort(ctx); err != nil {
				logger.Error("addon post-connect best-effort failed", "addon", spec.Descriptor.ID, "error", err)
			}
		}
	}
	return reverseHooks(), nil
}

// newProductionKeyResolver builds the built-in key registry resolver.
func newProductionKeyResolver() *domain.KeyResolver {
	return &domain.KeyResolver{
		Registries: domain.BuiltInKeyRegistries(),
		Clients: map[domain.KeyRegistryType]domain.RegistryClient{
			domain.KeyRegistryTypeGitHub: &keyregistry.GitHubClient{},
		},
	}
}

// oidcHTTPClientFromBundle builds an optional HTTP client with custom CA trust.
// Empty input returns nil (callers use the system trust store / default client).
func oidcHTTPClientFromBundle(oidcCABundle []byte) *http.Client {
	if len(oidcCABundle) == 0 {
		return nil
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	pool.AppendCertsFromPEM(oidcCABundle)
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool},
		},
	}
}
