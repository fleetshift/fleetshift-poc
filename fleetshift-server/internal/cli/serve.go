package cli

import (
	"context"
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
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/goworkflows"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/observability"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
)

type serveFlags struct {
	grpcAddr   string
	httpAddr   string
	dbPath     string
	logLevel   string
	logFormat  string
	oidcCAFile string
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

	logger, err := buildLogger(f.logLevel, f.logFormat)
	if err != nil {
		return err
	}

	kindOpts := []kindaddon.AgentOption{
		kindaddon.WithObserver(kindaddon.NewSlogAgentObserver(logger)),
	}
	if f.oidcCAFile != "" {
		caBundle, err := os.ReadFile(f.oidcCAFile)
		if err != nil {
			return fmt.Errorf("read OIDC CA file: %w", err)
		}
		kindOpts = append(kindOpts, kindaddon.WithOIDCCABundle(caBundle))
	}
	kindAgent := kindaddon.NewAgent(
		func(logger kindlog.Logger) kindaddon.ClusterProvider {
			return cluster.NewProvider(cluster.ProviderWithLogger(logger))
		},
		kindOpts...,
	)
	router.Register(kindaddon.TargetType, kindAgent)

	kubeAgent := kubernetesaddon.NewAgent()
	router.Register(kubernetesaddon.TargetType, kubeAgent)

	wfBackend := wfsqlite.NewSqliteBackend(f.dbPath,
		wfsqlite.WithBackendOptions(wfbackend.WithLogger(logger)),
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
		AcceptedResourceTypes: []domain.ResourceType{kindaddon.ClusterResourceType},
	}); err != nil && !errors.Is(err, domain.ErrAlreadyExists) {
		return fmt.Errorf("seed kind target: %w", err)
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()
	if err := wfWorker.Start(workerCtx); err != nil {
		return fmt.Errorf("start workflow worker: %w", err)
	}

	// --- auth infrastructure ---

	authMethodRepo := &sqlite.AuthMethodRepo{DB: db}
	discoveryClient := oidc.NewDiscoveryClient(nil)
	tokenVerifier, err := oidc.NewVerifier(ctx)
	if err != nil {
		return fmt.Errorf("create OIDC verifier: %w", err)
	}

	authMethodSvc := &application.AuthMethodService{
		Methods:   authMethodRepo,
		Discovery: discoveryClient,
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

	authnInterceptor := transportgrpc.NewAuthnInterceptor(authMethodSvc, tokenVerifier, observability.NewAuthnObserver(logger))

	// --- application services ---

	deploymentSvc := &application.DeploymentService{
		Store:    store,
		CreateWF: createWf,
	}

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

func buildLogger(level, format string) (*slog.Logger, error) {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "info", "":
		lv = slog.LevelInfo
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		return nil, fmt.Errorf("unknown log level %q (valid: debug, info, warn, error)", level)
	}

	opts := &slog.HandlerOptions{Level: lv}
	var handler slog.Handler
	switch strings.ToLower(format) {
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, opts)
	case "text", "":
		handler = slog.NewTextHandler(os.Stderr, opts)
	default:
		return nil, fmt.Errorf("unknown log format %q (valid: text, json)", format)
	}

	return slog.New(handler), nil
}
