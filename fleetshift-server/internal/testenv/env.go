// Package testenv is the runner-neutral FleetShift environment harness.
// Core APIs use context.Context and return errors; [testing.T] adapters
// live in thin helpers such as [StartT].
package testenv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/bootstrap"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery/fake"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
)

// Default call, eventual, startup, and teardown budgets.
const (
	DefaultCallTimeout     = 5 * time.Second
	DefaultEventualTimeout = 10 * time.Second
	DefaultEventualPoll    = 100 * time.Millisecond
	DefaultStartupTimeout  = 30 * time.Second
	DefaultTeardownTimeout = 15 * time.Second
)

// Env is a started test environment for a selected profile. Exercise the
// product through public endpoints; use Delivery/Inventory controllers
// only for stimuli that public APIs cannot cause.
type Env struct {
	Profile      string
	Capabilities []string
	Endpoints    bootstrap.Endpoints

	Delivery  *fake.Controller
	Inventory *InventoryController
	Artifacts *ArtifactBundle

	idp       *oidctest.Provider
	server    *bootstrap.Server
	logger    *slog.Logger
	dir       string
	startedAt time.Time
}

// Option configures [Start].
type Option func(*startConfig)

// startConfig holds options applied by [Start].
type startConfig struct {
	profile   string
	workDir   string
	logger    *slog.Logger
	artifacts string
}

// WithProfile selects an environment profile. Only [ProfileHermeticAPI]
// is implemented; other known names return an unsupported-profile error.
func WithProfile(name string) Option {
	return func(c *startConfig) { c.profile = name }
}

// WithWorkDir sets the private runtime directory (SQLite DB, and
// artifacts when [WithArtifactDir] is unset). When empty, Start creates
// a temporary directory; [Env.Close] does not remove it.
func WithWorkDir(dir string) Option {
	return func(c *startConfig) { c.workDir = dir }
}

// WithLogger substitutes the environment logger.
func WithLogger(logger *slog.Logger) Option {
	return func(c *startConfig) { c.logger = logger }
}

// WithArtifactDir sets the allow-listed artifact upload root. When empty,
// Start uses workDir/artifacts.
func WithArtifactDir(dir string) Option {
	return func(c *startConfig) { c.artifacts = dir }
}

// Start starts a runner-neutral environment. It returns only after
// listeners serve, migrations and the workflow runtime are ready, and
// every claimed capability is usable (including an authenticated
// capability probe for hermetic-api).
func Start(ctx context.Context, opts ...Option) (*Env, error) {
	cfg := startConfig{profile: ProfileHermeticAPI}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.profile == "" {
		cfg.profile = ProfileHermeticAPI
	}
	switch cfg.profile {
	case ProfileHermeticAPI:
	case "hermetic-ui", "kind", "oci-smoke", "production-deps", "keycloak-compat":
		return nil, fmt.Errorf("unsupported profile %q: not implemented", cfg.profile)
	default:
		return nil, fmt.Errorf("unknown profile %q", cfg.profile)
	}

	ownedDir := false
	dir := cfg.workDir
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "fleetshift-testenv-*")
		if err != nil {
			return nil, fmt.Errorf("testenv: create work dir: %w", err)
		}
		ownedDir = true
	}
	artifactDir := cfg.artifacts
	if artifactDir == "" {
		artifactDir = filepath.Join(dir, "artifacts")
	}
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		if ownedDir {
			_ = os.RemoveAll(dir)
		}
		return nil, fmt.Errorf("testenv: create artifact dir: %w", err)
	}

	logger := cfg.logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	env := &Env{
		Profile:   cfg.profile,
		logger:    logger,
		dir:       dir,
		startedAt: time.Now(),
		Artifacts: newArtifactBundle(artifactDir),
	}
	env.Artifacts.recordEvent("environment_start", map[string]any{
		"profile": cfg.profile,
	})

	if err := env.startHermeticAPI(ctx); err != nil {
		env.Artifacts.recordEvent("environment_start_failed", map[string]any{"error": err.Error()})
		_ = env.Close(context.Background())
		logger.Warn("testenv: hermetic start failed", "err", err)
		return nil, err
	}
	env.Capabilities = append([]string(nil), HermeticCapabilities...)
	env.Artifacts.recordEvent("environment_ready", map[string]any{
		"profile":      env.Profile,
		"capabilities": env.Capabilities,
		"grpc":         env.Endpoints.GRPC.Dial,
		"http":         env.Endpoints.HTTP.Dial,
	})
	if err := env.Artifacts.writeSummary(env, "started", nil); err != nil {
		logger.Warn("testenv: write artifact summary", "err", err)
	}
	return env, nil
}

// startHermeticAPI assembles the hermetic-api profile over bootstrap:
// oidctest identity, memworkflow, fake delivery, and controlled inventory.
func (e *Env) startHermeticAPI(ctx context.Context) error {
	idp, err := oidctest.New()
	if err != nil {
		return fmt.Errorf("start oidctest: %w", err)
	}
	e.idp = idp

	oidcDeps, err := bootstrap.NewProductionOIDCDeps(ctx, idp.HTTPClient())
	if err != nil {
		return fmt.Errorf("oidc deps: %w", err)
	}

	dbPath := filepath.Join(e.dir, "fleetshift.db")
	bcfg, err := bootstrap.NewConfig(bootstrap.ConfigInput{
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
		DBPath:   dbPath,
		// No production add-ons: trust-bundle placement stays unset so
		// anonymous CreateAuthMethod can complete without targets.
	})
	if err != nil {
		return fmt.Errorf("NewConfig: %w", err)
	}

	var deliveryCtrl *fake.Controller
	var inventoryCtrl *InventoryController

	srv, err := bootstrap.Start(ctx, bcfg, e.logger,
		bootstrap.WithWorkflowRegistry(bootstrap.NewMemWorkflowRegistry()),
		bootstrap.WithOIDCDeps(oidcDeps),
		bootstrap.WithAddonAssembly(func(_ context.Context, deps bootstrap.AddonDeps) ([]bootstrap.AddonSpec, error) {
			agent, ctrl := fake.New(deps.DeliveryReporter)
			deliveryCtrl = ctrl
			inventoryCtrl = NewInventoryController(deps.InventoryReporter, HermeticInventoryType)
			return []bootstrap.AddonSpec{{
				Descriptor: hermeticDescriptor(),
				Connect: application.ConnectInput{
					Agent:   agent,
					Targets: []domain.TargetInfo{hermeticTarget()},
					Schemas: []domain.ExtensionResourceSchema{hermeticInventorySchema()},
				},
			}}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("bootstrap.Start: %w", err)
	}
	e.server = srv
	e.Endpoints = srv.Endpoints()
	e.Delivery = deliveryCtrl
	e.Inventory = inventoryCtrl

	if err := e.bootstrapAuthAndProbe(ctx); err != nil {
		return err
	}
	return nil
}

// bootstrapAuthAndProbe registers the hermetic OIDC auth method through
// the public API and proves an authenticated ListDeployments call succeeds.
func (e *Env) bootstrapAuthAndProbe(ctx context.Context) error {
	conn, err := e.DialGRPC()
	if err != nil {
		return fmt.Errorf("grpc dial for readiness: %w", err)
	}
	defer conn.Close()

	authClient := pb.NewAuthMethodServiceClient(conn)
	callCtx, cancel := context.WithTimeout(ctx, DefaultCallTimeout)
	defer cancel()
	_, err = authClient.CreateAuthMethod(callCtx, &pb.CreateAuthMethodRequest{
		AuthMethodId: "hermetic-oidc",
		AuthMethod: &pb.AuthMethod{
			Type: pb.AuthMethod_TYPE_OIDC,
			OidcConfig: &pb.OIDCConfig{
				IssuerUrl:             string(e.idp.IssuerURL()),
				Audience:              string(e.idp.Audience()),
				KeyEnrollmentAudience: "fleetshift-signing",
			},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateAuthMethod via public API: %w", err)
	}
	e.Artifacts.recordEvent("auth_method_created", map[string]any{"id": "hermetic-oidc"})

	token, err := e.idp.Issue(oidctest.TokenClaims{Subject: "hermetic-readiness"})
	if err != nil {
		return fmt.Errorf("issue readiness token: %w", err)
	}
	probeCtx, probeCancel := context.WithTimeout(ctx, DefaultCallTimeout)
	defer probeCancel()
	probeCtx = metadata.NewOutgoingContext(probeCtx, metadata.Pairs("authorization", "Bearer "+token))
	_, err = pb.NewDeploymentServiceClient(conn).ListDeployments(probeCtx, &pb.ListDeploymentsRequest{})
	if err != nil {
		return fmt.Errorf("authenticated capability probe: %w", err)
	}
	return nil
}

// IssueToken mints a programmatic access token for the environment's
// identity provider.
func (e *Env) IssueToken(claims oidctest.TokenClaims) (string, error) {
	if e.idp == nil {
		return "", fmt.Errorf("testenv: identity provider not started")
	}
	if claims.Subject == "" {
		claims.Subject = "hermetic-user"
	}
	return e.idp.Issue(claims)
}

// DialGRPC returns a client connection to the resolved gRPC endpoint.
// Callers must close the connection.
func (e *Env) DialGRPC() (*grpc.ClientConn, error) {
	return grpc.NewClient(e.Endpoints.GRPC.Dial, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// AuthedContext attaches a bearer token to ctx for gRPC/HTTP calls.
func AuthedContext(ctx context.Context, token string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
}

// CallContext returns a child context bounded by the lesser of remaining
// parent deadline and DefaultCallTimeout.
func CallContext(parent context.Context) (context.Context, context.CancelFunc) {
	return boundedChild(parent, DefaultCallTimeout)
}

// PollCallContext returns a child context for one RPC/HTTP operation
// inside a poll, bounded by the remaining poll budget and DefaultCallTimeout.
func PollCallContext(pollCtx context.Context) (context.Context, context.CancelFunc) {
	return boundedChild(pollCtx, DefaultCallTimeout)
}

// boundedChild returns a child context whose deadline is the earlier of
// the parent's deadline and now+max.
func boundedChild(parent context.Context, max time.Duration) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(max)
	if d, ok := parent.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return context.WithDeadline(parent, deadline)
}

// Close shuts down the environment. It is bounded, idempotent, and
// records cleanup outcome in the artifact bundle.
func (e *Env) Close(ctx context.Context) error {
	if e == nil {
		return nil
	}
	var errs []error
	e.Artifacts.recordEvent("environment_close", map[string]any{
		"uptime": time.Since(e.startedAt).String(),
	})
	if e.server != nil {
		if err := e.server.Close(ctx); err != nil {
			errs = append(errs, err)
		}
		e.server = nil
	}
	if e.idp != nil {
		if err := e.idp.Close(); err != nil {
			errs = append(errs, err)
		}
		e.idp = nil
	}
	status := "closed"
	closeErr := errors.Join(errs...)
	if closeErr != nil {
		status = "close_error"
	}
	_ = e.Artifacts.writeSummary(e, status, closeErr)
	return closeErr
}
