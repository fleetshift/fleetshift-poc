package serverapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"buf.build/go/protovalidate"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	gcphcpaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	kubernetesaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/observability"
	pgstore "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/extensionresource"
	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
	transporthttp "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/http"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/platformresource"
)

// App is a fully ready FleetShift application. Endpoints are immutable.
// Stores, services, and servers are not exposed; exercise the app through
// public endpoints. Wait surfaces unexpected background termination; Close
// is concurrency-safe, idempotent, and bounded by the caller's context.
type App struct {
	endpoints Endpoints
	logger    *slog.Logger

	grpcServer *grpc.Server
	httpServer *http.Server
	grpcLis    net.Listener
	httpLis    net.Listener

	wfRuntime WorkflowRuntime
	appCtx    context.Context
	appCancel context.CancelFunc

	db                *sql.DB
	dynamicHTTPConn   *grpc.ClientConn
	kubeIndexing      *kubernetesInProcessIndexing
	indexReplayDone   <-chan struct{}
	authMethodSvc     *application.AuthMethodService
	shutdownGrace     time.Duration
	serveErrCh        chan error
	shutdownRequested bool
	ready             bool

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
	closeMu   sync.Mutex
}

// Endpoints returns immutable resolved listener addresses.
func (a *App) Endpoints() Endpoints { return a.endpoints }

// Wait blocks until an unexpected background termination occurs or Close
// completes. After normal Close, Wait returns the same terminal result.
func (a *App) Wait() error {
	select {
	case err := <-a.serveErrCh:
		a.closeMu.Lock()
		shutdown := a.shutdownRequested
		a.closeMu.Unlock()
		if shutdown && isExpectedServeStop(err) {
			<-a.closeDone
			return a.closeErr
		}
		if err != nil {
			return err
		}
		<-a.closeDone
		return a.closeErr
	case <-a.closeDone:
		return a.closeErr
	}
}

// Close performs bounded, idempotent shutdown. Concurrent callers share one
// cleanup execution; each waits only until its own context expires.
func (a *App) Close(ctx context.Context) error {
	a.closeMu.Lock()
	a.shutdownRequested = true
	a.closeMu.Unlock()

	a.closeOnce.Do(func() {
		a.closeErr = a.shutdown()
		close(a.closeDone)
	})

	select {
	case <-a.closeDone:
		return a.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Start eagerly constructs the complete production object graph and returns
// only when the application is semantically ready. The start context bounds
// construction and readiness; after success, long-lived work uses an
// app-owned context. On failure, acquired resources are cleaned up in reverse.
func Start(ctx context.Context, cfg Config, logger *slog.Logger, opts ...Option) (*App, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}
	if err := cfg.checkInvariants(); err != nil {
		return nil, err
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	appCtx, appCancel := context.WithCancel(context.Background())
	app := &App{
		logger:        logger,
		appCtx:        appCtx,
		appCancel:     appCancel,
		shutdownGrace: o.shutdownGrace,
		serveErrCh:    make(chan error, 2),
		closeDone:     make(chan struct{}),
	}

	var cleanups []func()
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	fail := func(err error) (*App, error) {
		appCancel()
		cleanup()
		return nil, err
	}

	// --- persistence ---
	var (
		db             *sql.DB
		store          domain.Store
		vault          domain.Vault
		authMethodRepo domain.AuthMethodRepository
		err            error
	)
	// activeResources backs QueryRepository's optional type-specific
	// field validation and DynamicSchemaActivator's activation state
	// (see [domain.QuerySchemaProvider] and
	// [extensionresource.ActiveResourceRegistry]). It starts empty and is
	// populated as managed resource schemas are activated below.
	activeResources := extensionresource.NewActiveResourceRegistry()

	switch database := cfg.Database.(type) {
	case Postgres:
		db, err = pgstore.Open(database.DriverDSN)
		if err != nil {
			return fail(fmt.Errorf("open database: %w", err))
		}
		store = &pgstore.Store{DB: db, SchemaProvider: activeResources}
		vault = &pgstore.VaultStore{DB: db}
		authMethodRepo = &pgstore.AuthMethodRepo{DB: db}
	case SQLite:
		db, err = sqlite.Open(database.Path)
		if err != nil {
			return fail(fmt.Errorf("open database: %w", err))
		}
		store = &sqlite.Store{DB: db, SchemaProvider: activeResources}
		vault = &sqlite.VaultStore{DB: db}
		authMethodRepo = &sqlite.AuthMethodRepo{DB: db}
	default:
		return fail(fmt.Errorf("unsupported database config %T", cfg.Database))
	}
	app.db = db
	cleanups = append(cleanups, func() { _ = db.Close() })

	// --- workflow runtime ---
	wfRuntime := o.workflowRuntime
	if wfRuntime == nil {
		wfRuntime, err = NewGoWorkflowRuntime(cfg.Database, logger)
		if err != nil {
			return fail(err)
		}
	}
	app.wfRuntime = wfRuntime
	reg := wfRuntime.Registry()

	specValidator, err := protovalidate.New()
	if err != nil {
		return fail(fmt.Errorf("create spec validator: %w", err))
	}

	router := delivery.NewRoutingDeliveryService()
	enabledAddons := cfg.AddonSet()
	logger.Info("enabled addons", "addons", slices.Sorted(maps.Keys(enabledAddons)))

	eventHub := transporthttp.NewEventHub(logger)
	deliveryReporter := application.NewDeliveryReportService(
		store,
		reg,
		application.WithDeliveryObserver(observability.NewMultiDeliveryObserver(
			observability.NewDeliveryObserver(logger),
			eventHub,
		)),
	)
	inventoryReportService := application.NewInventoryReportService(store)
	inventoryReporter := application.NewInventoryReporterAdapter(inventoryReportService)

	// --- kubernetes indexing runtime ---
	//
	// Built before Kind/GCP agents so those agents can receive an
	// IndexingRuntime and call EnsureIndexer / StopIndexer. With that
	// runtime injected, indexers start from those agents before Delivered
	// and from a one-shot startup replay after addon connect.
	// Orchestration does not start or stop indexers.
	var kubeIndexing *kubernetesInProcessIndexing
	if enabledAddons[AddonKubernetes] {
		kubeIndexing = newKubernetesInProcessIndexing(appCtx, store, vault, logger)
		app.kubeIndexing = kubeIndexing
		cleanups = append(cleanups, func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := kubeIndexing.Runtime.StopAll(stopCtx); err != nil {
				logger.Error("kubernetes index StopAll error", "error", err)
			}
		})
	}

	// --- identity ---
	identity := o.identity
	if identity == nil {
		id, err := NewProductionIdentity(ctx, cfg.OIDCCABundle)
		if err != nil {
			return fail(err)
		}
		identity = &id
	}

	keyResolver := newProductionKeyResolver()
	oidcHTTPClient := oidcHTTPClientFromBundle(cfg.OIDCCABundle)

	// --- register all workflows before starting the worker ---
	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		domain.WithFulfillmentObserver(observability.NewFulfillmentObserver(logger)),
		domain.WithVault(vault),
	)
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		return fail(fmt.Errorf("register orchestration: %w", err))
	}

	createWf, err := reg.RegisterCreateDeployment(&domain.CreateDeploymentWorkflowSpec{
		Store: store, Orchestration: orchWf,
	})
	if err != nil {
		return fail(fmt.Errorf("register create-deployment: %w", err))
	}

	deleteObs := observability.NewDeleteObserver(logger)
	cleanupWf, err := reg.RegisterDeleteDeploymentCleanup(&domain.DeleteDeploymentCleanupWorkflowSpec{
		Store: store, Observer: deleteObs,
	})
	if err != nil {
		return fail(fmt.Errorf("register delete-deployment-cleanup: %w", err))
	}
	deleteWf, err := reg.RegisterDeleteDeployment(&domain.DeleteDeploymentWorkflowSpec{
		Store: store, Orchestration: orchWf, Cleanup: cleanupWf, Observer: deleteObs,
	})
	if err != nil {
		return fail(fmt.Errorf("register delete-deployment: %w", err))
	}

	createMRWf, err := reg.RegisterCreateManagedResource(&domain.CreateManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf,
	})
	if err != nil {
		return fail(fmt.Errorf("register create-managed-resource: %w", err))
	}
	mrCleanupWf, err := reg.RegisterDeleteManagedResourceCleanup(&domain.DeleteManagedResourceCleanupWorkflowSpec{
		Store: store, Observer: deleteObs,
	})
	if err != nil {
		return fail(fmt.Errorf("register delete-managed-resource-cleanup: %w", err))
	}
	deleteMRWf, err := reg.RegisterDeleteManagedResource(&domain.DeleteManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf, Cleanup: mrCleanupWf, Observer: deleteObs,
	})
	if err != nil {
		return fail(fmt.Errorf("register delete-managed-resource: %w", err))
	}

	setupHub := transporthttp.NewSetupHub(logger)
	var gcphcpTargetID string
	if enabledAddons[AddonGCPHCP] {
		gcphcpCfg, err := gcphcpaddon.ParseConfig(cfg.GCPHCPConfigPath)
		if err != nil {
			return fail(fmt.Errorf("parse gcphcp config: %w", err))
		}
		gcphcpTargetID = gcphcpCfg.Targets[0].ID
	}
	provSpec := &domain.ProvisionIdPWorkflowSpec{
		AuthMethods:      authMethodRepo,
		Discovery:        identity.Discovery,
		CreateDeployment: createWf,
		EventSink:        setupHub,
	}
	if placement := buildTrustBundlePlacement(enabledAddons, gcphcpTargetID); placement.Type != "" {
		provSpec.TrustBundlePlacement = placement
	}
	// Facade assemblies may enable kind/gcphcp without Config.Addons; trust
	// placement for those is handled when the facade sets Config.Addons to match.
	provWf, err := reg.RegisterProvisionIdP(provSpec)
	if err != nil {
		return fail(fmt.Errorf("register provision-idp: %w", err))
	}

	authMethodSvc := &application.AuthMethodService{
		Methods:     authMethodRepo,
		ProvisionWF: provWf,
	}
	app.authMethodSvc = authMethodSvc

	existingMethods, err := authMethodSvc.List(ctx)
	if err != nil {
		return fail(fmt.Errorf("load auth methods: %w", err))
	}
	registerPersistedKeySets(ctx, logger, identity.Verifier, existingMethods)

	authnInterceptor := transportgrpc.NewAuthnInterceptor(authMethodSvc, identity.Verifier, observability.NewAuthnObserver(logger))

	provenanceSvc := &domain.ProvenanceService{
		KeyResolver: keyResolver,
		AuthMethods: authMethodRepo,
	}
	resumeWf, err := reg.RegisterResumeDeployment(&domain.ResumeDeploymentWorkflowSpec{
		Store: store, Orchestration: orchWf, ProvenanceSvc: provenanceSvc,
	})
	if err != nil {
		return fail(fmt.Errorf("register resume-deployment: %w", err))
	}
	resumeMRWf, err := reg.RegisterResumeManagedResource(&domain.ResumeManagedResourceWorkflowSpec{
		Store: store, Orchestration: orchWf, ProvenanceSvc: provenanceSvc,
	})
	if err != nil {
		return fail(fmt.Errorf("register resume-managed-resource: %w", err))
	}

	resourceQuerySvc := application.NewResourceQueryService(store)
	deploymentSvc := &application.DeploymentService{
		Store: store, CreateWF: createWf, DeleteWF: deleteWf, ResumeWF: resumeWf, ProvenanceSvc: provenanceSvc,
	}
	signerEnrollmentSvc := &application.SignerEnrollmentService{
		Store: store, Verifier: identity.Verifier, AuthMethods: authMethodRepo,
	}
	extensionResourceSvc := application.NewExtensionResourceService(
		store, createMRWf, deleteMRWf, resumeMRWf, provenanceSvc,
	)

	// --- transport ---
	dynamicMux := dynamicapi.NewDynamicServiceMux()
	fileRegistry := dynamicapi.NewDynamicFileRegistry()
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(authnInterceptor.Unary()),
		grpc.ChainStreamInterceptor(authnInterceptor.Stream()),
		grpc.UnknownServiceHandler(dynamicMux.Handle),
	)
	pb.RegisterDeploymentServiceServer(grpcServer, &transportgrpc.DeploymentServer{Deployments: deploymentSvc})
	pb.RegisterAuthMethodServiceServer(grpcServer, &transportgrpc.AuthMethodServer{
		AuthMethods: authMethodSvc,
		Authn:       authnInterceptor,
	})
	pb.RegisterSignerEnrollmentServiceServer(grpcServer, &transportgrpc.SignerEnrollmentServer{
		Enrollments: signerEnrollmentSvc,
	})
	pb.RegisterResourceQueryServiceServer(grpcServer, &transportgrpc.ResourceQueryServer{
		Queries:  resourceQuerySvc,
		Registry: activeResources,
	})
	dynamicapi.RegisterCompositeReflection(grpcServer, dynamicMux, fileRegistry)
	app.grpcServer = grpcServer

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fail(fmt.Errorf("listen gRPC on %s: %w", cfg.GRPCAddr, err))
	}
	app.grpcLis = grpcLis
	cleanups = append(cleanups, func() { _ = grpcLis.Close() })
	grpcEP, err := endpointFromListener(grpcLis)
	if err != nil {
		return fail(err)
	}

	gwMux := runtime.NewServeMux()
	gwOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if err := pb.RegisterDeploymentServiceHandlerFromEndpoint(ctx, gwMux, grpcEP.Dial, gwOpts); err != nil {
		return fail(fmt.Errorf("register deployment gateway: %w", err))
	}
	if err := pb.RegisterAuthMethodServiceHandlerFromEndpoint(ctx, gwMux, grpcEP.Dial, gwOpts); err != nil {
		return fail(fmt.Errorf("register auth method gateway: %w", err))
	}
	if err := pb.RegisterSignerEnrollmentServiceHandlerFromEndpoint(ctx, gwMux, grpcEP.Dial, gwOpts); err != nil {
		return fail(fmt.Errorf("register signer enrollment gateway: %w", err))
	}
	if err := pb.RegisterResourceQueryServiceHandlerFromEndpoint(ctx, gwMux, grpcEP.Dial, gwOpts); err != nil {
		return fail(fmt.Errorf("register resource query gateway: %w", err))
	}

	// Dynamic managed resource HTTP routes are registered directly on
	// topMux by the SchemaActivator at canonical
	// /apis/{service}/{version}/{collection} prefixes. Go 1.22+ ServeMux
	// uses longest-prefix matching, so these always take precedence over
	// the gateway's /v1/ catch-all and the platform-owned
	// /apis/fleetshift.io/ prefix used by QueryResources.
	topMux := http.NewServeMux()
	topMux.Handle("/v1/", gwMux)
	topMux.Handle("/apis/fleetshift.io/", gwMux)

	// HTTP auth middleware — mirrors the gRPC authn interceptor: if
	// auth methods are configured require a valid OIDC Bearer token,
	// otherwise allow anonymous (setup mode). Applied selectively to
	// endpoints that need protection; /api/ui/config,
	// /api/ui/setup/ws, and /api/ui/events/ws intentionally remain
	// unauthenticated (events/ws because the browser WebSocket API
	// cannot set Authorization headers — see TODO below).
	httpAuthn := &transporthttp.AuthnMiddleware{
		Methods:  authMethodSvc,
		Verifier: identity.Verifier,
		Logger:   logger.With("component", "authn-http"),
	}
	topMux.HandleFunc("GET /api/ui/setup/ws", setupHub.HandleWS)
	// TODO(auth): Browser WebSocket API cannot set custom HTTP headers, so
	// wrapping this endpoint with httpAuthn.Wrap would always 401 once OIDC
	// is configured. Proper WS auth requires a short-lived OTP/ticket
	// handshake — passing the JWT as a query param leaks into logs,
	// referrer, and browser history. Leave unauthenticated for now.
	topMux.HandleFunc("GET /api/ui/events/ws", eventHub.HandleWS)
	topMux.Handle("GET /api/ui/github-signing-keys/{username}", httpAuthn.Wrap(http.HandlerFunc(transporthttp.HandleGitHubSigningKeys)))
	topMux.Handle("POST /api/ui/verify-sign", &transporthttp.VerifySignHandler{
		AuthMethods: authMethodSvc, Verifier: identity.Verifier, Store: store, ProvenanceSvc: provenanceSvc,
	})

	dynamicHTTPConn, err := grpc.NewClient(grpcEP.Dial, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fail(fmt.Errorf("dynamic http mux grpc client: %w", err))
	}
	app.dynamicHTTPConn = dynamicHTTPConn
	cleanups = append(cleanups, func() { _ = dynamicHTTPConn.Close() })
	dynamicHTTPMux := dynamicapi.NewDynamicHTTPMux(topMux, dynamicHTTPConn)

	if cfg.WebDir != "" {
		uiMux := transporthttp.NewUIConfigMux(transporthttp.UIConfigOptions{
			WebDir:         cfg.WebDir,
			OIDCAuthority:  cfg.OIDCUIAuthority,
			OIDCUIClientID: cfg.OIDCUIClientID,
			Logger:         logger,
			AuthMiddleware: httpAuthn.Wrap,
			AuthConfigured: func(ctx context.Context) (bool, error) {
				methods, err := authMethodSvc.List(ctx)
				if err != nil {
					return false, err
				}
				for _, m := range methods {
					if m.Type() == domain.AuthMethodTypeOIDC && m.OIDC() != nil {
						return true, nil
					}
				}
				return false, nil
			},
		})
		topMux.Handle("/api/ui/", uiMux)
		topMux.Handle("/", transporthttp.NewStaticHandler(cfg.WebDir))
		logger.Info("serving frontend assets", "web-dir", cfg.WebDir)
	}

	httpLis, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fail(fmt.Errorf("listen HTTP on %s: %w", cfg.HTTPAddr, err))
	}
	app.httpLis = httpLis
	cleanups = append(cleanups, func() { _ = httpLis.Close() })
	httpEP, err := endpointFromListener(httpLis)
	if err != nil {
		return fail(err)
	}
	app.endpoints = Endpoints{GRPC: grpcEP, HTTP: httpEP}
	app.httpServer = &http.Server{Handler: transporthttp.MaxBody(topMux)}

	// --- addon lifecycle ---
	//
	// Enable then Connect before Serve so schemas/targets/agents are ready
	// when the first request arrives. (Older serve connected after listen;
	// that is no longer needed because proveReadiness gates Start return.)
	typeSvc := application.NewExtensionResourceTypeService(store)
	platformResourceSvc := application.NewPlatformResourceService(store)
	activator := &extensionresource.DynamicSchemaActivator{
		GRPCMux:      dynamicMux,
		HTTPMux:      dynamicHTTPMux,
		FileRegistry: fileRegistry,
		Deps: extensionresource.Deps{
			Resources: extensionResourceSvc,
			Validator: specValidator,
		},
		PlatformDeps: platformresource.Deps{Resources: platformResourceSvc},
		Registry:     activeResources,
	}
	addonMgr := application.NewAddonManager(application.AddonManagerDeps{
		Router: router, TypeSvc: typeSvc, Activator: activator,
	})

	addonDeps := AddonDeps{
		Config:            cfg,
		Logger:            logger,
		Store:             store,
		Vault:             vault,
		DeliveryReporter:  deliveryReporter,
		InventoryReporter: inventoryReporter,
		OIDCCABundle:      cfg.OIDCCABundle,
		Indexing:          kubeIndexing,
		IndexCtx:          appCtx,
	}

	var specs []AddonSpec
	if o.addonAssembly != nil {
		specs, err = o.addonAssembly(ctx, addonDeps)
	} else {
		specs, err = assembleProductionAddons(addonDeps, keyResolver, oidcHTTPClient)
	}
	if err != nil {
		return fail(err)
	}

	for _, spec := range specs {
		if err := addonMgr.Enable(ctx, spec.Descriptor); err != nil {
			return fail(fmt.Errorf("enable %s addon: %w", spec.Descriptor.ID, err))
		}
		if err := rejectNilClaimedAgent(spec); err != nil {
			return fail(err)
		}
		if err := addonMgr.Connect(ctx, spec.Descriptor.ID, spec.Connect); err != nil {
			return fail(fmt.Errorf("connect %s addon: %w", spec.Descriptor.ID, err))
		}
		if spec.AfterConnect != nil {
			if err := spec.AfterConnect(ctx); err != nil {
				return fail(err)
			}
		}
		if spec.AfterConnectWarn != nil {
			if err := spec.AfterConnectWarn(ctx); err != nil {
				logger.Error("addon post-connect warning", "addon", spec.Descriptor.ID, "error", err)
			}
		}
	}

	// One-shot startup replay recovers persisted Kubernetes targets; it must
	// not block listen/readiness. Close joins the replay goroutine before StopAll.
	if kubeIndexing != nil {
		replayDone := startKubernetesIndexStartupReplay(appCtx, func(replayCtx context.Context) {
			kubernetesaddon.ReplayPersistedIndexers(
				replayCtx,
				storeTargetLister{store: store},
				vault,
				kubeIndexing.Runtime,
				logger,
			)
		})
		app.indexReplayDone = replayDone
		logger.Info("kubernetes index startup replay started")
	}

	// --- start workflow runtime and servers ---
	if err := wfRuntime.Start(appCtx); err != nil {
		return fail(err)
	}
	cleanups = append(cleanups, func() {
		waitCtx, cancel := context.WithTimeout(context.Background(), o.shutdownGrace)
		defer cancel()
		_ = wfRuntime.Close(waitCtx)
	})

	go func() {
		logger.Info("gRPC server listening", "addr", grpcEP.Bind, "dial", grpcEP.Dial)
		err := grpcServer.Serve(grpcLis)
		app.serveErrCh <- err
	}()
	go func() {
		logger.Info("HTTP gateway listening", "addr", httpEP.Bind, "dial", httpEP.Dial)
		err := app.httpServer.Serve(httpLis)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			app.serveErrCh <- err
		}
	}()

	if err := proveReadiness(ctx, grpcEP.Dial); err != nil {
		return fail(err)
	}
	app.ready = true

	// Successful start: transfer lifetime ownership; do not run fail cleanups.
	cleanups = nil
	return app, nil
}

// rejectNilClaimedAgent is a placeholder for delivery-agent presence checks.
// It currently always returns nil: focused facade assemblies may claim
// DeliveryCapability with a nil Connect.Agent (schemas/targets only).
// Production assembly still supplies non-nil agents.
func rejectNilClaimedAgent(spec AddonSpec) error {
	for _, cap := range spec.Descriptor.Capabilities {
		if _, ok := cap.(domain.DeliveryCapability); ok {
			if spec.Connect.Agent == nil {
				return nil
			}
		}
	}
	return nil
}

// proveReadiness dials gRPC and calls ListDeployments. Success, Unauthenticated,
// and other non-Unavailable status codes count as ready (routing/middleware
// proved). Unavailable and non-status transport errors fail Start.
func proveReadiness(ctx context.Context, grpcDial string) error {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(grpcDial, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("readiness dial: %w", err)
	}
	defer conn.Close()

	client := pb.NewDeploymentServiceClient(conn)
	_, err = client.ListDeployments(probeCtx, &pb.ListDeploymentsRequest{})
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if ok && st.Code() == codes.Unauthenticated {
		// Expected when auth methods are configured and no credential is presented.
		return nil
	}
	// Empty bootstrap (no auth methods) allows anonymous; other errors fail readiness.
	if ok && st.Code() == codes.Unavailable {
		return fmt.Errorf("readiness probe: %w", err)
	}
	// PermissionDenied / other app errors still prove routing/middleware.
	if ok {
		return nil
	}
	return fmt.Errorf("readiness probe: %w", err)
}

// isExpectedServeStop reports whether err is a normal listener stop during
// Close (nil, http.ErrServerClosed, or grpc.ErrServerStopped).
func isExpectedServeStop(err error) bool {
	if err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped) {
		return true
	}
	// grpc.Serve returns nil on GracefulStop; Forced stop may return errors.
	return false
}

// shutdown stops ingress, cancels app-owned work, joins index replay and
// workflow runtime, and closes connections/DB. Invoked once from Close.
func (a *App) shutdown() error {
	a.logger.Info("shutting down")
	a.ready = false

	var primary error
	join := func(err error) {
		if err == nil {
			return
		}
		if primary == nil {
			primary = err
			return
		}
		primary = errors.Join(primary, err)
	}

	// Quiesce ingress while dependencies remain live.
	stopped := make(chan struct{})
	go func() {
		a.grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(a.shutdownGrace):
		a.grpcServer.Stop()
		<-stopped
	}

	httpCtx, httpCancel := context.WithTimeout(context.Background(), a.shutdownGrace)
	defer httpCancel()
	if a.httpServer != nil {
		join(a.httpServer.Shutdown(httpCtx))
	}

	// Cancel app-owned work and join producers.
	a.appCancel()
	if a.indexReplayDone != nil {
		select {
		case <-a.indexReplayDone:
		case <-time.After(a.shutdownGrace):
			join(fmt.Errorf("kubernetes index replay join timed out"))
		}
	}
	if a.kubeIndexing != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), a.shutdownGrace)
		join(a.kubeIndexing.Runtime.StopAll(stopCtx))
		cancel()
	}
	if a.wfRuntime != nil {
		waitCtx, cancel := context.WithTimeout(context.Background(), a.shutdownGrace)
		join(a.wfRuntime.Close(waitCtx))
		cancel()
	}
	if a.dynamicHTTPConn != nil {
		join(a.dynamicHTTPConn.Close())
	}
	if a.grpcLis != nil {
		_ = a.grpcLis.Close()
	}
	if a.httpLis != nil {
		_ = a.httpLis.Close()
	}
	if a.db != nil {
		join(a.db.Close())
	}
	return primary
}
