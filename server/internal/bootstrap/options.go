package bootstrap

import (
	"context"
	"log/slog"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Option configures a cohesive composition substitution for Start.
// Per-service, per-route, or per-server overrides are prohibited.
type Option func(*options)

// options is the unexported Start option bag.
type options struct {
	workflowRegistry domain.Registry
	oidcDeps         *OIDCDeps
	addonAssembly    AddonAssemblyFunc

	shutdownGrace time.Duration
}

// defaultOptions returns production Start defaults.
func defaultOptions() options {
	return options{
		shutdownGrace: 5 * time.Second,
	}
}

// WithWorkflowRegistry substitutes the workflow registry (registration and
// Start/Wait/Close lifecycle). When omitted, Start builds the production
// go-workflows registry from Config.Database. reg must be non-nil.
func WithWorkflowRegistry(reg domain.Registry) Option {
	if reg == nil {
		panic("bootstrap.WithWorkflowRegistry: registry is nil")
	}
	return func(o *options) { o.workflowRegistry = reg }
}

// WithOIDCDeps substitutes discovery and token verification dependencies.
// When omitted, Start builds the production OIDC discovery client and verifier.
func WithOIDCDeps(deps OIDCDeps) Option {
	return func(o *options) { o.oidcDeps = &deps }
}

// WithAddonAssembly substitutes ordered typed add-on assembly. When omitted,
// Start builds production agents from Config.Addons. The assembly still flows
// through AddonManager.Enable/Connect.
func WithAddonAssembly(fn AddonAssemblyFunc) Option {
	return func(o *options) { o.addonAssembly = fn }
}

// AddonAssemblyFunc builds ordered add-on specs after persistence and shared
// deps exist. It must not start background work that outlives Start.
type AddonAssemblyFunc func(ctx context.Context, deps AddonDeps) ([]AddonSpec, error)

// AddonDeps carries shared inputs required to assemble production or test add-ons.
type AddonDeps struct {
	Config            Config
	Logger            *slog.Logger
	Store             domain.Store
	Vault             domain.Vault
	DeliveryReporter  domain.DeliveryReporter
	InventoryReporter domain.InventoryReporter
	// Indexing is non-nil when the kubernetes add-on is being assembled for production.
	Indexing *kubernetesInProcessIndexing
	// AppCtx is the application-owned context for addon work; cancelled on
	// server Close. Add-ons should use this for background work that must
	// be cancellable during shutdown.
	AppCtx context.Context
}

// AddonSpec is one Enable/Connect unit. Connect still uses the production
// AddonManager lifecycle.
type AddonSpec struct {
	Descriptor domain.AddonDescriptor
	Connect    application.ConnectInput
	// AfterConnect runs after a successful Connect. Failures fail Start.
	// Use for work that must complete before readiness (not GCP recovery).
	AfterConnect func(ctx context.Context) error
	// AfterConnectBestEffort runs after Connect; errors are logged and ignored
	// (GCP recovery compatibility).
	AfterConnectBestEffort func(ctx context.Context) error
	// Close is called during shutdown in reverse registration order, after
	// appCtx has been cancelled. It should observe the cancellation and join
	// any in-flight work started with appCtx, ensuring they terminate before
	// returning. A nil Close is valid and means the addon has no background
	// work to join.
	Close func(ctx context.Context) error
}
