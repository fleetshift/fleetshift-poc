package bootstrap

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
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/extensionresource"
	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
	transporthttp "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/http"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/platformresource"
)

// Server is a fully ready FleetShift process handle. Endpoints are immutable.
// Stores, services, and transport servers are not exposed; exercise the
// process through public endpoints. Wait surfaces unexpected background
// termination; Close is concurrency-safe, idempotent, and bounded by the
// caller's context.
type Server struct {
	endpoints Endpoints
	logger    *slog.Logger

	grpcServer *grpc.Server
	httpServer *http.Server
	grpcLis    net.Listener
	httpLis    net.Listener

	wfRegistry domain.Registry
	appCtx     context.Context
	appCancel  context.CancelFunc

	db                *sql.DB
	dynamicHTTPConn   *grpc.ClientConn
	kubeIndexing      *kubernetesInProcessIndexing
	indexReplayDone   <-chan struct{}
	shutdownGrace     time.Duration
	serveErrCh        chan error
	shutdownRequested bool
	readiness         *transporthttp.Readiness

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
	closeMu   sync.Mutex
}

// Endpoints returns immutable resolved listener addresses.
func (s *Server) Endpoints() Endpoints { return s.endpoints }

// Wait blocks until an unexpected background termination occurs or Close
// completes. After normal Close, Wait returns the same terminal result.
func (s *Server) Wait() error {
	select {
	case err := <-s.serveErrCh:
		s.closeMu.Lock()
		shutdown := s.shutdownRequested
		s.closeMu.Unlock()
		if shutdown && isExpectedServeStop(err) {
			<-s.closeDone
			return s.closeErr
		}
		if err != nil {
			return err
		}
		<-s.closeDone
		return s.closeErr
	case <-s.closeDone:
		return s.closeErr
	}
}

// Close performs bounded, idempotent shutdown. Concurrent callers share one
// cleanup execution; the first caller runs shutdown to completion, and later
// callers observe the same terminal result. Caller ctx does not cancel
// in-flight shared cleanup.
//
// TODO: Close(ctx) does not honestly bound the Once winner — shutdown ignores
// ctx and the post-Do select rarely observes deadline. Revisit whether ctx
// should be documented as advisory-only, dropped, or made a real wait budget.
func (s *Server) Close(ctx context.Context) error {
	s.closeMu.Lock()
	s.shutdownRequested = true
	s.closeMu.Unlock()

	s.closeOnce.Do(func() {
		s.closeErr = s.shutdown()
		close(s.closeDone)
	})

	select {
	case <-s.closeDone:
		return s.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Start eagerly constructs the complete production object graph and returns
// only when the application is semantically ready. The start context bounds
// construction and readiness; after success, long-lived work uses an
// app-owned context. On failure, acquired resources are cleaned up in reverse.
func Start(ctx context.Context, cfg Config, logger *slog.Logger, opts ...Option) (*Server, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	appCtx, appCancel := context.WithCancel(context.Background())
	srv := &Server{
		logger:        logger,
		appCtx:        appCtx,
		appCancel:     appCancel,
		shutdownGrace: o.shutdownGrace,
		serveErrCh:    make(chan error, 2),
		closeDone:     make(chan struct{}),
		readiness:     &transporthttp.Readiness{},
	}

	var cleanups []func()
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	fail := func(err error) (*Server, error) {
		appCancel()
		cleanup()
		return nil, err
	}

	// --- persistence ---
	p, err := openPersistence(cfg.Database)
	if err != nil {
		return fail(err)
	}
	srv.db = p.db
	cleanups = append(cleanups, func() { _ = p.db.Close() })

	// --- workflow registry ---
	reg := o.workflowRegistry
	if reg == nil {
		reg, err = NewGoWorkflowRegistry(cfg.Database, logger)
		if err != nil {
			return fail(err)
		}
	}
	srv.wfRegistry = reg

	specValidator, err := protovalidate.New()
	if err != nil {
		return fail(fmt.Errorf("create spec validator: %w", err))
	}

	router := delivery.NewRoutingDeliveryService()
	enabledAddons := cfg.AddonSet()
	logger.Info("enabled addons", "addons", slices.Sorted(maps.Keys(enabledAddons)))

	eventHub := transporthttp.NewEventHub(logger)
	inventoryReportService := application.NewInventoryReportService(p.store)
	deliveryReporter := application.NewDeliveryReportService(
		p.store,
		reg,
		application.WithDeliveryObserver(observability.NewMultiDeliveryObserver(
			observability.NewDeliveryObserver(logger),
			eventHub,
		)),
	)
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
		kubeIndexing = newKubernetesInProcessIndexing(appCtx, p.vault, inventoryReportService, logger)
		srv.kubeIndexing = kubeIndexing
		cleanups = append(cleanups, func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := kubeIndexing.Runtime.StopAll(stopCtx); err != nil {
				logger.Error("kubernetes index StopAll error", "error", err)
			}
		})
	}

	// --- OIDC deps (shared HTTP client for discovery, verifier, kube agent) ---
	oidcHTTPClient := oidcHTTPClientFromBundle(cfg.OIDCCABundle)
	oidcDeps := o.oidcDeps
	if oidcDeps == nil {
		deps, err := NewProductionOIDCDeps(ctx, oidcHTTPClient)
		if err != nil {
			return fail(err)
		}
		oidcDeps = &deps
	}

	keyResolver := newProductionKeyResolver()

	// Parse GCP config once when enabled (trust placement + production assembly).
	var gcphcpCfg *gcphcpaddon.Config
	var gcphcpTargetID string
	if enabledAddons[AddonGCPHCP] {
		parsed, err := gcphcpaddon.ParseConfig(cfg.GCPHCPConfigPath)
		if err != nil {
			return fail(fmt.Errorf("parse gcphcp config: %w", err))
		}
		gcphcpCfg = &parsed
		gcphcpTargetID = parsed.Targets[0].ID
	}

	// --- register all workflows before starting the worker ---
	setupHub := transporthttp.NewSetupHub(logger)
	wfs, err := registerWorkflows(
		reg, p.store, p.vault, router, p.authMethodRepo, *oidcDeps, setupHub,
		enabledAddons, gcphcpTargetID, keyResolver, logger,
	)
	if err != nil {
		return fail(err)
	}

	initialAuthMethod, err := prepareAuthMethods(ctx, cfg, wfs.authMethodSvc, oidcDeps.Verifier, logger)
	if err != nil {
		return fail(err)
	}

	authnInterceptor := transportgrpc.NewAuthnInterceptor(wfs.authMethodSvc, oidcDeps.Verifier, observability.NewAuthnObserver(logger))
	resourceQuerySvc := application.NewResourceQueryService(p.store)
	deploymentSvc := &application.DeploymentService{
		Store: p.store, CreateWF: wfs.createWf, DeleteWF: wfs.deleteWf, ResumeWF: wfs.resumeWf, ProvenanceSvc: wfs.provenanceSvc,
	}
	signerEnrollmentSvc := &application.SignerEnrollmentService{
		Store: p.store, Verifier: oidcDeps.Verifier, AuthMethods: p.authMethodRepo,
	}
	extensionResourceSvc := application.NewExtensionResourceService(
		p.store, wfs.createMRWf, wfs.deleteMRWf, wfs.resumeMRWf, wfs.provenanceSvc,
	)

	// --- transport ---
	dynamicMux := dynamicapi.NewDynamicServiceMux()
	fileRegistry := dynamicapi.NewDynamicFileRegistry()
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(authnInterceptor.Unary()),
		grpc.ChainStreamInterceptor(authnInterceptor.Stream()),
		grpc.UnknownServiceHandler(dynamicMux.Handle),
	)
	registerStaticGRPCServices(
		grpcServer, deploymentSvc, wfs.authMethodSvc, authnInterceptor,
		signerEnrollmentSvc, resourceQuerySvc, p.activeResources, dynamicMux, fileRegistry,
	)
	srv.grpcServer = grpcServer

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fail(fmt.Errorf("listen gRPC on %s: %w", cfg.GRPCAddr, err))
	}
	srv.grpcLis = grpcLis
	cleanups = append(cleanups, func() { _ = grpcLis.Close() })
	grpcEP := endpointFromListener(grpcLis)

	// Shared loopback ClientConn for static gRPC-gateway handlers and the
	// dynamic HTTP mux. Owned by Close (via dynamicHTTPConn); must not use
	// HandlerFromEndpoint with the start ctx, which would close the conn when
	// that ctx ends (e.g. testserver.Start's defer cancel).
	dynamicHTTPConn, err := grpc.NewClient(grpcEP.Dial, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fail(fmt.Errorf("loopback grpc client: %w", err))
	}
	srv.dynamicHTTPConn = dynamicHTTPConn
	cleanups = append(cleanups, func() { _ = dynamicHTTPConn.Close() })

	gwMux := runtime.NewServeMux()
	if err := registerGatewayHandlers(ctx, gwMux, dynamicHTTPConn); err != nil {
		return fail(err)
	}

	// Dynamic managed resource HTTP routes are registered directly on
	// topMux by the SchemaActivator at canonical
	// /apis/{service}/{version}/{collection} prefixes. Go 1.22+ ServeMux
	// uses longest-prefix matching, so these always take precedence over
	// the gateway's /v1/ catch-all and the platform-owned
	// /apis/fleetshift.io/ prefix used by QueryResources.
	topMux := http.NewServeMux()
	// Minimal health probes are unauthenticated and registered before auth
	// middleware / SPA fallback.
	transporthttp.RegisterHealthRoutes(topMux, srv.readiness)
	topMux.Handle("/v1/", gwMux)
	topMux.Handle("/apis/fleetshift.io/", gwMux)

	if err := registerUIHTTP(topMux, uiHTTPDeps{
		cfg:           cfg,
		logger:        logger,
		authMethods:   wfs.authMethodSvc,
		verifier:      oidcDeps.Verifier,
		store:         p.store,
		provenanceSvc: wfs.provenanceSvc,
		setupHub:      setupHub,
		eventHub:      eventHub,
	}); err != nil {
		return fail(err)
	}

	dynamicHTTPMux := dynamicapi.NewDynamicHTTPMux(topMux, dynamicHTTPConn)

	httpLis, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fail(fmt.Errorf("listen HTTP on %s: %w", cfg.HTTPAddr, err))
	}
	srv.httpLis = httpLis
	cleanups = append(cleanups, func() { _ = httpLis.Close() })
	httpEP := endpointFromListener(httpLis)
	srv.endpoints = Endpoints{GRPC: grpcEP, HTTP: httpEP}
	srv.httpServer = &http.Server{Handler: transporthttp.MaxBody(topMux)}

	// --- addon lifecycle ---
	//
	// Enable then Connect before Serve so schemas/targets/agents are ready
	// when the first request arrives. (Older serve connected after listen;
	// that is no longer needed because proveReadiness gates Start return.)
	typeSvc := application.NewExtensionResourceTypeService(p.store)
	platformResourceSvc := application.NewPlatformResourceService(p.store)
	activator := &extensionresource.DynamicSchemaActivator{
		GRPCMux:      dynamicMux,
		HTTPMux:      dynamicHTTPMux,
		FileRegistry: fileRegistry,
		Deps: extensionresource.Deps{
			Resources: extensionResourceSvc,
			Validator: specValidator,
		},
		PlatformDeps: platformresource.Deps{Resources: platformResourceSvc},
		Registry:     p.activeResources,
	}
	addonMgr := application.NewAddonManager(application.AddonManagerDeps{
		Router: router, TypeSvc: typeSvc, Activator: activator,
	})

	addonDeps := AddonDeps{
		Config:            cfg,
		Logger:            logger,
		Store:             p.store,
		Vault:             p.vault,
		DeliveryReporter:  deliveryReporter,
		InventoryReporter: inventoryReporter,
		Indexing:          kubeIndexing,
		IndexCtx:          appCtx,
	}

	var specs []AddonSpec
	if o.addonAssembly != nil {
		specs, err = o.addonAssembly(ctx, addonDeps)
	} else {
		specs, err = assembleProductionAddons(addonDeps, keyResolver, oidcHTTPClient, gcphcpCfg)
	}
	if err != nil {
		return fail(err)
	}
	if err := enableAndConnectAddons(ctx, addonMgr, specs, logger); err != nil {
		return fail(err)
	}

	// One-shot startup replay recovers persisted Kubernetes targets; it must
	// not block listen/readiness. Close joins the replay goroutine before StopAll.
	if kubeIndexing != nil {
		replayDone := startKubernetesIndexStartupReplay(appCtx, func(replayCtx context.Context) {
			kubernetesaddon.ReplayPersistedIndexers(
				replayCtx,
				storeTargetLister{store: p.store},
				p.vault,
				kubeIndexing.Runtime,
				logger,
			)
		})
		srv.indexReplayDone = replayDone
		logger.Info("kubernetes index startup replay started")
	}

	// --- start workflow registry and servers ---
	if err := reg.Start(appCtx); err != nil {
		return fail(err)
	}
	cleanups = append(cleanups, func() {
		waitCtx, cancel := context.WithTimeout(context.Background(), o.shutdownGrace)
		defer cancel()
		_ = reg.Close(waitCtx)
	})

	// Install the initial AuthMethod after the workflow worker is running so
	// ProvisionIdP can complete under both mem and go-workflows registries.
	// Listeners are bound but Serve has not started, so the public API stays closed.
	if initialAuthMethod != nil {
		if err := initialAuthMethod.Install(ctx, logger); err != nil {
			return fail(err)
		}
	}

	go func() {
		logger.Info("gRPC server listening", "addr", grpcEP.Bind, "dial", grpcEP.Dial)
		err := grpcServer.Serve(grpcLis)
		srv.serveErrCh <- err
	}()
	go func() {
		logger.Info("HTTP gateway listening", "addr", httpEP.Bind, "dial", httpEP.Dial)
		err := srv.httpServer.Serve(httpLis)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			srv.serveErrCh <- err
		}
	}()
	// Serve owns accepted connections; register stop before readiness so a
	// failed proveReadiness unwinds servers the same way as Close.
	cleanups = append(cleanups, func() {
		_ = stopIngress(grpcServer, srv.httpServer, o.shutdownGrace)
	})

	if err := proveReadiness(ctx, grpcEP.Dial); err != nil {
		return fail(err)
	}
	srv.readiness.MarkReady()

	// Successful start: transfer lifetime ownership; do not run fail cleanups.
	cleanups = nil
	return srv, nil
}

// rejectNilClaimedAgent fails Start when a DeliveryCapability is claimed
// without a Connect.Agent. A claimed capability must be routable.
func rejectNilClaimedAgent(spec AddonSpec) error {
	for _, cap := range spec.Descriptor.Capabilities {
		if _, ok := cap.(domain.DeliveryCapability); ok {
			if spec.Connect.Agent == nil {
				return fmt.Errorf("addon %q claims delivery capability but Connect.Agent is nil", spec.Descriptor.ID)
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
func (s *Server) shutdown() error {
	s.logger.Info("shutting down")
	if s.readiness != nil {
		s.readiness.ClearReady()
	}

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
	join(stopIngress(s.grpcServer, s.httpServer, s.shutdownGrace))

	// Cancel app-owned work and join producers.
	s.appCancel()
	if s.indexReplayDone != nil {
		select {
		case <-s.indexReplayDone:
		case <-time.After(s.shutdownGrace):
			join(fmt.Errorf("kubernetes index replay join timed out"))
		}
	}
	if s.kubeIndexing != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), s.shutdownGrace)
		join(s.kubeIndexing.Runtime.StopAll(stopCtx))
		cancel()
	}
	if s.wfRegistry != nil {
		waitCtx, cancel := context.WithTimeout(context.Background(), s.shutdownGrace)
		join(s.wfRegistry.Close(waitCtx))
		cancel()
	}
	if s.dynamicHTTPConn != nil {
		join(s.dynamicHTTPConn.Close())
	}
	if s.grpcLis != nil {
		_ = s.grpcLis.Close()
	}
	if s.httpLis != nil {
		_ = s.httpLis.Close()
	}
	if s.db != nil {
		join(s.db.Close())
		s.db = nil
	}
	return primary
}

// stopIngress performs graceful-then-forced gRPC stop and bounded HTTP
// Shutdown. Used by Close and by Start fail cleanup after Serve has begun.
func stopIngress(grpcServer *grpc.Server, httpServer *http.Server, grace time.Duration) error {
	var primary error
	if grpcServer != nil {
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(grace):
			grpcServer.Stop()
			<-stopped
		}
	}
	if httpServer != nil {
		httpCtx, cancel := context.WithTimeout(context.Background(), grace)
		if err := httpServer.Shutdown(httpCtx); err != nil {
			primary = err
		}
		cancel()
	}
	return primary
}
