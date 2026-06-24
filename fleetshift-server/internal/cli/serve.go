package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"buf.build/go/protovalidate"
	wfbackend "github.com/cschleiden/go-workflows/backend"
	wfpostgres "github.com/cschleiden/go-workflows/backend/postgres"
	wfsqlite "github.com/cschleiden/go-workflows/backend/sqlite"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/goworkflows"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/observability"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc"
	pgstore "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/slogutil"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
	transporthttp "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/http"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/managedresource"
)

type serveFlags struct {
	grpcAddr         string
	httpAddr         string
	dbPath           string
	databaseURL      string
	databaseURLFile  string
	logLevel         string
	logFormat        string
	logLevelOverride string
	oidcCAFile       string
	webDir           string
	oidcUIAuthority  string
	oidcUIClientID   string
	addons           string
	gcphcpConfig     string
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
	cmd.Flags().StringVar(&f.databaseURL, "database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL (mutually exclusive with --db)")
	cmd.Flags().StringVar(&f.databaseURLFile, "database-url-file", os.Getenv("DATABASE_URL_FILE"), "path to file containing PostgreSQL connection URL (mutually exclusive with --database-url and --db)")
	cmd.Flags().StringVar(&f.logLevel, "log-level", "info", "log level (debug, info, warn, error)")
	cmd.Flags().StringVar(&f.logFormat, "log-format", "text", "log format (text, json)")
	cmd.Flags().StringVar(&f.logLevelOverride, "log-level-override", "", "per-component log level overrides (e.g. deployment=debug,authn=debug)")
	cmd.Flags().StringVar(&f.oidcCAFile, "oidc-ca-file", "", "PEM CA certificate for OIDC issuers (for kind clusters trusting self-signed or local CAs)")
	cmd.Flags().StringVar(&f.webDir, "web-dir", "", "directory containing frontend assets to serve (empty = API only)")
	cmd.Flags().StringVar(&f.oidcUIAuthority, "oidc-ui-authority", os.Getenv("OIDC_ISSUER_URL"), "OIDC authority URL for the frontend UI")
	cmd.Flags().StringVar(&f.oidcUIClientID, "oidc-ui-client-id", envOrDefault("OIDC_UI_CLIENT_ID", "fleetshift-ui"), "OIDC client ID for the frontend UI")
	cmd.Flags().StringVar(&f.addons, "addons", "kind,kubernetes,gcphcp", "comma-separated list of addons to enable (default: all)")
	cmd.Flags().StringVar(&f.gcphcpConfig, "gcphcp-config", "", "path to gcphcp addon config file (or GCPHCP_CONFIG env)")
	return cmd
}

func runServe(ctx context.Context, f *serveFlags) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := resolveDatabaseURLFile(f); err != nil {
		return err
	}

	// --- infrastructure ---
	if f.databaseURL != "" && f.dbPath != "fleetshift.db" {
		return fmt.Errorf("--database-url and --db are mutually exclusive")
	}

	var (
		db             *sql.DB
		store          domain.Store
		vault          domain.Vault
		authMethodRepo domain.AuthMethodRepository
	)

	if f.databaseURL != "" {
		var err error
		db, err = pgstore.Open(f.databaseURL)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		store = &pgstore.Store{DB: db}
		vault = &pgstore.VaultStore{DB: db}
		authMethodRepo = &pgstore.AuthMethodRepo{DB: db}
	} else {
		var err error
		db, err = sqlite.Open(f.dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		store = &sqlite.Store{DB: db}
		vault = &sqlite.VaultStore{DB: db}
		authMethodRepo = &sqlite.AuthMethodRepo{DB: db}
	}
	defer db.Close()

	specValidator, err := protovalidate.New()
	if err != nil {
		return fmt.Errorf("create spec validator: %w", err)
	}

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

	enabledAddons := parseAddons(f.addons)
	logger.Info("enabled addons", "addons", slices.Sorted(maps.Keys(enabledAddons)))

	var wfBackend wfbackend.Backend
	if f.databaseURL != "" {
		pgHost, pgPort, pgUser, pgPass, pgDB, err := parseDatabaseURL(f.databaseURL)
		if err != nil {
			return fmt.Errorf("parse database URL for workflows backend: %w", err)
		}
		wfBackend = wfpostgres.NewPostgresBackend(pgHost, pgPort, pgUser, pgPass, pgDB,
			wfpostgres.WithBackendOptions(wfbackend.WithLogger(logger.With("component", "workflows"))),
		)
	} else {
		wfBackend = wfsqlite.NewSqliteBackend(f.dbPath,
			wfsqlite.WithBackendOptions(wfbackend.WithLogger(logger.With("component", "workflows"))),
		)
	}
	wfWorker := worker.New(wfBackend, nil)
	wfClient := client.New(wfBackend)

	reg := &goworkflows.Registry{
		Worker:  wfWorker,
		Client:  wfClient,
		Timeout: 30 * time.Second,
	}

	eventHub := transporthttp.NewEventHub(logger)

	deliveryReporter := application.NewDeliveryReportService(
		store,
		reg,
		application.WithDeliveryObserver(observability.NewMultiDeliveryObserver(
			observability.NewDeliveryObserver(logger),
			eventHub,
		)),
	)

	keyResolver := &domain.KeyResolver{
		Registries: domain.BuiltInKeyRegistries(),
		Clients: map[domain.KeyRegistryType]domain.RegistryClient{
			domain.KeyRegistryTypeGitHub: &keyregistry.GitHubClient{},
		},
	}

	// --- construct addon agents ---
	//
	// Agent construction stays here because agents have external
	// dependencies (Docker, AWS creds, etc.) that the addon manager
	// should not own. Registration with the router and targets happens
	// later via AddonManager.Connect / RegisterTarget.
	addons, err := constructAddons(ctx, enabledAddons, f, logger, deliveryReporter, store, vault, keyResolver, oidcHTTPClient, oidcCABundle)
	if err != nil {
		return err
	}

	typeSvc := &application.ManagedResourceTypeService{Store: store}
	addonMgr := application.NewAddonManager(application.AddonManagerDeps{
		Router:           router,
		TypeSvc:          typeSvc,
		Activator:        nil, // set later after dynamic service infrastructure
		InventoryCleanup: &application.InventoryCleanupService{Store: store},
	})

	orchOpts := []domain.OrchestrationWorkflowOption{
		domain.WithFulfillmentObserver(observability.NewFulfillmentObserver(logger)),
		domain.WithVault(vault),
		domain.WithTargetObserver(addonMgr),
	}

	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		orchOpts...,
	)
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

	cleanupSpec := &domain.DeleteDeploymentCleanupWorkflowSpec{
		Store: store,
	}
	cleanupWf, err := reg.RegisterDeleteDeploymentCleanup(cleanupSpec)
	if err != nil {
		return fmt.Errorf("register delete-deployment-cleanup: %w", err)
	}

	deleteSpec := &domain.DeleteDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Cleanup:       cleanupWf,
	}
	deleteWf, err := reg.RegisterDeleteDeployment(deleteSpec)
	if err != nil {
		return fmt.Errorf("register delete-deployment: %w", err)
	}

	// --- managed resource workflows ---

	createMRSpec := &domain.CreateManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	createMRWf, err := reg.RegisterCreateManagedResource(createMRSpec)
	if err != nil {
		return fmt.Errorf("register create-managed-resource: %w", err)
	}

	mrCleanupSpec := &domain.DeleteManagedResourceCleanupWorkflowSpec{
		Store: store,
	}
	mrCleanupWf, err := reg.RegisterDeleteManagedResourceCleanup(mrCleanupSpec)
	if err != nil {
		return fmt.Errorf("register delete-managed-resource-cleanup: %w", err)
	}

	deleteMRSpec := &domain.DeleteManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Cleanup:       mrCleanupWf,
	}
	deleteMRWf, err := reg.RegisterDeleteManagedResource(deleteMRSpec)
	if err != nil {
		return fmt.Errorf("register delete-managed-resource: %w", err)
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()
	if err := wfWorker.Start(workerCtx); err != nil {
		return fmt.Errorf("start workflow worker: %w", err)
	}

	// --- auth infrastructure ---

	discoveryClient := oidc.NewDiscoveryClient(oidcHTTPClient)

	var verifierOpts []oidc.VerifierOption
	if oidcHTTPClient != nil {
		verifierOpts = append(verifierOpts, oidc.WithHTTPClient(oidcHTTPClient))
	}
	tokenVerifier, err := oidc.NewVerifier(ctx, verifierOpts...)
	if err != nil {
		return fmt.Errorf("create OIDC verifier: %w", err)
	}

	setupHub := transporthttp.NewSetupHub(logger)
	provSpec := &domain.ProvisionIdPWorkflowSpec{
		AuthMethods:      authMethodRepo,
		Discovery:        discoveryClient,
		CreateDeployment: createWf,
		EventSink:        setupHub,
	}
	// TODO: change in future to support multiple gcphcp targets
	var gcphcpTargetID string
	if enabledAddons["gcphcp"] {
		gcphcpTargetID = addons.gcphcpCfg.Targets[0].ID
	}
	if placement := buildTrustBundlePlacement(enabledAddons, gcphcpTargetID); placement.Type != "" {
		provSpec.TrustBundlePlacement = placement
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
		if m.Type() == domain.AuthMethodTypeOIDC && m.OIDC() != nil {
			if err := tokenVerifier.RegisterKeySet(ctx, m.OIDC().JWKSURI); err != nil {
				logger.Warn("failed to register JWKS for auth method", "id", m.ID(), "err", err)
			}
		}
	}

	authnInterceptor := transportgrpc.NewAuthnInterceptor(authMethodSvc, tokenVerifier, observability.NewAuthnObserver(logger))

	// --- application services ---

	provenanceSvc := &domain.ProvenanceService{
		KeyResolver: keyResolver,
		AuthMethods: authMethodRepo,
	}

	resumeSpec := &domain.ResumeDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		ProvenanceSvc: provenanceSvc,
	}
	resumeWf, err := reg.RegisterResumeDeployment(resumeSpec)
	if err != nil {
		return fmt.Errorf("register resume-deployment: %w", err)
	}

	resumeMRSpec := &domain.ResumeManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		ProvenanceSvc: provenanceSvc,
	}
	resumeMRWf, err := reg.RegisterResumeManagedResource(resumeMRSpec)
	if err != nil {
		return fmt.Errorf("register resume-managed-resource: %w", err)
	}

	deploymentSvc := &application.DeploymentService{
		Store:         store,
		CreateWF:      createWf,
		DeleteWF:      deleteWf,
		ResumeWF:      resumeWf,
		ProvenanceSvc: provenanceSvc,
	}

	signerEnrollmentSvc := &application.SignerEnrollmentService{
		Store:       store,
		Verifier:    tokenVerifier,
		AuthMethods: authMethodRepo,
	}

	managedResourceSvc := &application.ManagedResourceService{
		Store:         store,
		CreateWF:      createMRWf,
		DeleteWF:      deleteMRWf,
		ResumeWF:      resumeMRWf,
		ProvenanceSvc: provenanceSvc,
	}

	// --- dynamic service infrastructure ---

	dynamicMux := managedresource.NewDynamicServiceMux()
	fileRegistry := managedresource.NewDynamicFileRegistry()

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(authnInterceptor.Unary()),
		grpc.ChainStreamInterceptor(authnInterceptor.Stream()),
		grpc.UnknownServiceHandler(dynamicMux.Handle),
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
	managedresource.RegisterCompositeReflection(grpcServer, dynamicMux, fileRegistry)

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

	// Dynamic managed resource HTTP routes are registered directly on
	// topMux by the SchemaActivator at canonical
	// /apis/{service}/{version}/{collection} prefixes. Go 1.22+ ServeMux
	// uses longest-prefix matching, so these always take precedence over
	// the gateway's /v1/ catch-all.
	topMux := http.NewServeMux()
	topMux.Handle("/v1/", gwMux)
	topMux.HandleFunc("GET /api/ui/setup/ws", setupHub.HandleWS)
	topMux.HandleFunc("GET /api/ui/events/ws", eventHub.HandleWS)
	topMux.HandleFunc("GET /api/ui/github-signing-keys/{username}", transporthttp.HandleGitHubSigningKeys)
	topMux.Handle("POST /api/ui/verify-sign", &transporthttp.VerifySignHandler{
		AuthMethods:   authMethodSvc,
		Verifier:      tokenVerifier,
		Store:         store,
		ProvenanceSvc: provenanceSvc,
	})
	dynamicHTTPConn, err := grpc.NewClient(f.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dynamic http mux grpc client: %w", err)
	}
	defer dynamicHTTPConn.Close()
	dynamicHTTPMux := managedresource.NewDynamicHTTPMux(topMux, dynamicHTTPConn)

	if f.webDir != "" {
		uiMux := transporthttp.NewUIConfigMux(transporthttp.UIConfigOptions{
			WebDir:         f.webDir,
			OIDCAuthority:  f.oidcUIAuthority,
			OIDCUIClientID: f.oidcUIClientID,
			Logger:         logger,
		})
		topMux.Handle("/api/ui/", uiMux)
		topMux.Handle("/", transporthttp.NewStaticHandler(f.webDir))
		logger.Info("serving frontend assets", "web-dir", f.webDir)
	}

	httpServer := &http.Server{
		Addr:    f.httpAddr,
		Handler: topMux,
	}

	// --- addon lifecycle ---

	// Wire the schema activator into the AddonManager created earlier
	activator := &managedresource.DynamicSchemaActivator{
		GRPCMux:      dynamicMux,
		HTTPMux:      dynamicHTTPMux,
		FileRegistry: fileRegistry,
		Deps: managedresource.Deps{
			Resources: managedResourceSvc,
			Validator: specValidator,
		},
	}
	addonMgr.SetActivator(activator)

	// Phase 2 (Defined → Enabled): authorize addons and record capability
	// expectations. For backend capabilities (delivery agents, managed
	// resource schemas), the capability is recorded but not yet activated —
	// runtime assets come later at Connect.
	if err := enableAddons(ctx, addonMgr, enabledAddons); err != nil {
		return err
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

	// Phase 3 (Enabled → Connected): addon workloads provide runtime
	// assets — delivery agents, managed resource schemas, and target
	// definitions. Schemas are compiled, delivery agents registered in the
	// router, targets seeded. Happens AFTER the servers are serving so the
	// DynamicServiceMux can dispatch immediately.
	if err := connectAddons(ctx, addonMgr, enabledAddons, addons, logger); err != nil {
		return err
	}

	// Recover targets AFTER addons are connected (IndexAgents must be registered first)
	if err := orchSpec.RecoverTargets(ctx); err != nil {
		logger.Error("target recovery errors", "error", err)
	}

	// --- shutdown ---

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errCh:
		return err
	}

	shutdownAddons(addons)
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

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func resolveDatabaseURLFile(f *serveFlags) error {
	if f.databaseURLFile == "" {
		return nil
	}
	if f.databaseURL != "" {
		return fmt.Errorf("--database-url-file and --database-url are mutually exclusive")
	}
	if f.dbPath != "fleetshift.db" {
		return fmt.Errorf("--database-url-file and --db are mutually exclusive")
	}
	data, err := os.ReadFile(f.databaseURLFile)
	if err != nil {
		return fmt.Errorf("read database URL file: %w", err)
	}
	f.databaseURL = strings.TrimSpace(string(data))
	return nil
}

// parseDatabaseURL extracts host, port, user, password, and dbname from a
// PostgreSQL connection URL (e.g. "postgres://user:pass@host:5432/dbname").
func parseDatabaseURL(rawURL string) (host string, port int, user, password, dbname string, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", 0, "", "", "", fmt.Errorf("parse database URL: %w", err)
	}

	host = u.Hostname()
	portStr := u.Port()
	if portStr == "" {
		port = 5432
	} else {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return "", 0, "", "", "", fmt.Errorf("parse database port %q: %w", portStr, err)
		}
	}

	user = u.User.Username()
	password, _ = u.User.Password()
	dbname = strings.TrimPrefix(u.Path, "/")

	return host, port, user, password, dbname, nil
}
