package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	wfbackend "github.com/cschleiden/go-workflows/backend"
	wfsqlite "github.com/cschleiden/go-workflows/backend/sqlite"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/spf13/cobra"
	"sigs.k8s.io/kind/pkg/cluster"
	kindlog "sigs.k8s.io/kind/pkg/log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	kubernetesaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	ocpaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/ocp"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/goworkflows"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/observability"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/slogutil"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
)

type serveFlags struct {
	grpcAddr         string
	httpAddr         string
	dbPath           string
	logLevel         string
	logFormat        string
	logLevelOverride string
	oidcCAFile       string
}

func newServeCmd() *cobra.Command {
	f := &serveFlags{}
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the FleetShift gRPC and HTTP servers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServe(cmd.Context(), f)
		},
	}
	cmd.Flags().StringVar(&f.grpcAddr, "grpc-addr", ":50051", "gRPC listen address")
	cmd.Flags().StringVar(&f.httpAddr, "http-addr", ":8080", "HTTP/JSON gateway listen address")
	cmd.Flags().StringVar(&f.dbPath, "db", "fleetshift.db", "SQLite database path")
	cmd.Flags().StringVar(&f.logLevel, "log-level", "info", "log level (debug, info, warn, error)")
	cmd.Flags().StringVar(&f.logFormat, "log-format", "text", "log format (text, json)")
	cmd.Flags().StringVar(&f.logLevelOverride, "log-level-override", "", "per-component log level overrides (e.g. deployment=debug,authn=debug)")
	cmd.Flags().StringVar(&f.oidcCAFile, "oidc-ca-file", "", "PEM CA certificate for OIDC issuers (for kind clusters trusting self-signed or local CAs)")
	return cmd
}

func runServe(ctx context.Context, f *serveFlags) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- infrastructure ---

	db, err := sqlite.Open(f.dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	store := &sqlite.Store{DB: db}
	vault := &sqlite.VaultStore{DB: db}

	router := delivery.NewRoutingDeliveryService()

	logger, err := buildLogger(f.logLevel, f.logFormat, f.logLevelOverride)
	if err != nil {
		return err
	}

	var oidcCABundle []byte
	if f.oidcCAFile != "" {
		var err error
		oidcCABundle, err = os.ReadFile(f.oidcCAFile)
		if err != nil {
			return fmt.Errorf("read OIDC CA file: %w", err)
		}
	}

	kindOpts := []kindaddon.AgentOption{
		kindaddon.WithObserver(kindaddon.NewSlogAgentObserver(logger)),
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
	kindAgent := kindaddon.NewAgent(
		func(logger kindlog.Logger) kindaddon.ClusterProvider {
			return cluster.NewProvider(cluster.ProviderWithLogger(logger))
		},
		kindOpts...,
	)
	router.Register(kindaddon.TargetType, kindAgent)

	// --- OCP agent ---
	callbackSigner, err := ocpaddon.NewCallbackTokenSigner()
	if err != nil {
		return fmt.Errorf("create callback token signer: %w", err)
	}

	ocpCredProvider := &ocpaddon.PassthroughCredentialProvider{}
	ocpAgent := ocpaddon.NewAgent(
		ocpaddon.WithCallbackAddr(f.grpcAddr),
		ocpaddon.WithVault(vault),
		ocpaddon.WithCredentialProvider(ocpCredProvider),
		ocpaddon.WithTokenSigner(callbackSigner),
		ocpaddon.WithObserver(ocpaddon.NewSlogAgentObserver(logger)),
	)
	router.Register(ocpaddon.TargetType, ocpAgent)

	// Kubernetes agent is registered after the attestation verifier is
	// built (see below). The router is only consulted at delivery time.

	wfBackend := wfsqlite.NewSqliteBackend(f.dbPath,
		wfsqlite.WithBackendOptions(wfbackend.WithLogger(logger.With("component", "workflows"))),
	)
	wfWorker := worker.New(wfBackend, nil)
	wfClient := client.New(wfBackend)

	reg := &goworkflows.Registry{
		Worker:  wfWorker,
		Client:  wfClient,
		Timeout: 30 * time.Second,
	}

	orchSpec := &domain.OrchestrationWorkflowSpec{
		Store:            store,
		Delivery:         router,
		Strategies:       domain.DefaultStrategyFactory{},
		Registry:         reg,
		Observer:         observability.NewDeploymentObserver(logger),
		DeliveryObserver: observability.NewDeliveryObserver(logger),
		Vault:            vault,
	}
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		return fmt.Errorf("register orchestration: %w", err)
	}

	cwfSpec := &domain.CreateDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	createWf, err := reg.RegisterCreateDeployment(cwfSpec)
	if err != nil {
		return fmt.Errorf("register create-deployment: %w", err)
	}
	// --- seed default targets ---

	targetSvc := &application.TargetService{Store: store}
	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:                    "kind-local",
		Type:                  kindaddon.TargetType,
		Name:                  "Local Kind Provider",
		AcceptedResourceTypes: []domain.ResourceType{kindaddon.ClusterResourceType, domain.TrustBundleResourceType},
	}); err != nil && !errors.Is(err, domain.ErrAlreadyExists) {
		return fmt.Errorf("seed kind target: %w", err)
	}

	if err := targetSvc.Register(ctx, domain.TargetInfo{
		ID:                    "ocp-aws",
		Type:                  ocpaddon.TargetType,
		Name:                  "OCP on AWS",
		AcceptedResourceTypes: []domain.ResourceType{ocpaddon.ClusterResourceType},
	}); err != nil && !errors.Is(err, domain.ErrAlreadyExists) {
		return fmt.Errorf("seed ocp-aws target: %w", err)
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()
	if err := wfWorker.Start(workerCtx); err != nil {
		return fmt.Errorf("start workflow worker: %w", err)
	}

	// --- auth infrastructure ---

	var oidcHTTPClient *http.Client
	if oidcCABundle != nil {
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		pool.AppendCertsFromPEM(oidcCABundle)
		oidcHTTPClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: pool},
			},
		}
	}

	authMethodRepo := &sqlite.AuthMethodRepo{DB: db}
	discoveryClient := oidc.NewDiscoveryClient(oidcHTTPClient)

	var verifierOpts []oidc.VerifierOption
	if oidcHTTPClient != nil {
		verifierOpts = append(verifierOpts, oidc.WithHTTPClient(oidcHTTPClient))
	}
	tokenVerifier, err := oidc.NewVerifier(ctx, verifierOpts...)
	if err != nil {
		return fmt.Errorf("create OIDC verifier: %w", err)
	}

	provSpec := &domain.ProvisionIdPWorkflowSpec{
		AuthMethods:      authMethodRepo,
		Discovery:        discoveryClient,
		CreateDeployment: createWf,
		TrustBundlePlacement: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"kind-local"},
		},
	}
	provWf, err := reg.RegisterProvisionIdP(provSpec)
	if err != nil {
		return fmt.Errorf("register provision-idp: %w", err)
	}

	authMethodSvc := &application.AuthMethodService{
		Methods:     authMethodRepo,
		ProvisionWF: provWf,
	}

	existingMethods, err := authMethodSvc.List(ctx)
	if err != nil {
		return fmt.Errorf("load auth methods: %w", err)
	}
	for _, m := range existingMethods {
		if m.Type == domain.AuthMethodTypeOIDC && m.OIDC != nil {
			if err := tokenVerifier.RegisterKeySet(ctx, m.OIDC.JWKSURI); err != nil {
				logger.Warn("failed to register JWKS for auth method", "id", m.ID, "err", err)
			}
		}
	}

	authnInterceptor := transportgrpc.NewAuthnInterceptor(authMethodSvc, tokenVerifier, observability.NewAuthnObserver(logger),
		transportgrpc.WithSkipMethods(
			"/fleetshift.v1.OCPEngineCallbackService/ReportPhaseResult",
			"/fleetshift.v1.OCPEngineCallbackService/ReportMilestone",
			"/fleetshift.v1.OCPEngineCallbackService/ReportCompletion",
			"/fleetshift.v1.OCPEngineCallbackService/ReportFailure",
		),
	)

	// --- application services ---

	keyResolver := &application.KeyResolver{
		Registries: domain.BuiltInKeyRegistries(),
		Clients: map[domain.KeyRegistryType]domain.RegistryClient{
			domain.KeyRegistryTypeGitHub: &keyregistry.GitHubClient{},
		},
	}

	deploymentSvc := &application.DeploymentService{
		Store:         store,
		CreateWF:      createWf,
		Orchestration: orchWf,
		KeyResolver:   keyResolver,
	}

	signerEnrollmentSvc := &application.SignerEnrollmentService{
		Store:       store,
		Verifier:    tokenVerifier,
		AuthMethods: authMethodRepo,
	}

	// --- kubernetes delivery agent ---

	kubeAgentOpts := []kubernetesaddon.AgentOption{
		kubernetesaddon.WithKeyResolver(keyResolver),
		kubernetesaddon.WithVault(vault),
	}
	if oidcHTTPClient != nil {
		kubeAgentOpts = append(kubeAgentOpts, kubernetesaddon.WithHTTPClient(oidcHTTPClient))
	}
	kubeAgent := kubernetesaddon.NewAgent(kubeAgentOpts...)
	router.Register(kubernetesaddon.TargetType, kubeAgent)

	// --- gRPC server ---

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(authnInterceptor.Unary()),
		grpc.ChainStreamInterceptor(authnInterceptor.Stream()),
	)
	pb.RegisterDeploymentServiceServer(grpcServer, &transportgrpc.DeploymentServer{
		Deployments: deploymentSvc,
	})
	pb.RegisterAuthMethodServiceServer(grpcServer, &transportgrpc.AuthMethodServer{
		AuthMethods: authMethodSvc,
		Authn:       authnInterceptor,
	})
	pb.RegisterSignerEnrollmentServiceServer(grpcServer, &transportgrpc.SignerEnrollmentServer{
		Enrollments: signerEnrollmentSvc,
	})
	pb.RegisterOCPEngineCallbackServiceServer(grpcServer, ocpAgent.CallbackServer())
	reflection.Register(grpcServer)

	grpcLis, err := net.Listen("tcp", f.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC on %s: %w", f.grpcAddr, err)
	}

	// --- HTTP gateway ---

	gwMux := runtime.NewServeMux()
	gwOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if err := pb.RegisterDeploymentServiceHandlerFromEndpoint(ctx, gwMux, f.grpcAddr, gwOpts); err != nil {
		return fmt.Errorf("register deployment gateway: %w", err)
	}
	if err := pb.RegisterAuthMethodServiceHandlerFromEndpoint(ctx, gwMux, f.grpcAddr, gwOpts); err != nil {
		return fmt.Errorf("register auth method gateway: %w", err)
	}
	if err := pb.RegisterSignerEnrollmentServiceHandlerFromEndpoint(ctx, gwMux, f.grpcAddr, gwOpts); err != nil {
		return fmt.Errorf("register signer enrollment gateway: %w", err)
	}

	httpServer := &http.Server{
		Addr:    f.httpAddr,
		Handler: gwMux,
	}

	// --- start ---

	errCh := make(chan error, 2)

	go func() {
		logger.Info("gRPC server listening", "addr", f.grpcAddr)
		errCh <- grpcServer.Serve(grpcLis)
	}()

	go func() {
		logger.Info("HTTP gateway listening", "addr", f.httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// --- shutdown ---

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errCh:
		return err
	}

	grpcServer.GracefulStop()

	workerCancel()
	if err := wfWorker.WaitForCompletion(); err != nil {
		logger.Error("workflow worker shutdown error", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP shutdown error", "error", err)
	}

	return nil
}

func buildLogger(level, format, overrideSpec string) (*slog.Logger, error) {
	base, err := parseLevel(level)
	if err != nil {
		return nil, err
	}

	overrides, err := parseLevelOverrides(overrideSpec)
	if err != nil {
		return nil, err
	}

	// The inner handler's level must be the minimum of the base and all
	// overrides so it never prematurely rejects records an override wants.
	innerLevel := base
	for _, lvl := range overrides {
		if lvl < innerLevel {
			innerLevel = lvl
		}
	}

	opts := &slog.HandlerOptions{Level: innerLevel}
	var inner slog.Handler
	switch strings.ToLower(format) {
	case "json":
		inner = slog.NewJSONHandler(os.Stderr, opts)
	case "text", "":
		inner = slog.NewTextHandler(os.Stderr, opts)
	default:
		return nil, fmt.Errorf("unknown log format %q (valid: text, json)", format)
	}

	handler := slogutil.NewLevelOverrideHandler(inner, base, overrides)
	return slog.New(handler), nil
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q (valid: debug, info, warn, error)", s)
	}
}

// parseLevelOverrides parses a comma-separated string of component=level
// pairs (e.g. "deployment=debug,authn=warn").
func parseLevelOverrides(spec string) (map[slogutil.ComponentName]slog.Level, error) {
	if spec == "" {
		return nil, nil
	}
	overrides := make(map[slogutil.ComponentName]slog.Level)
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		k, v, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("invalid log level override %q: expected component=level", entry)
		}
		lvl, err := parseLevel(v)
		if err != nil {
			return nil, fmt.Errorf("invalid log level override %q: %w", entry, err)
		}
		overrides[slogutil.ComponentName(k)] = lvl
	}
	return overrides, nil
}
