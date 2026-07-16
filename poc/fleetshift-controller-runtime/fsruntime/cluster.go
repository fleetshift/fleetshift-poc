// Package fsruntime implements a controller-runtime cluster.Cluster (and a
// thin manager.Manager) backed by an in-memory store instead of
// kube-apiserver. It follows the same swap pattern as
// github.com/jmelis/postgres-controller-backend/pkg/pgruntime: reconciler
// code and SetupWithManager stay unchanged; only the manager constructor
// differs.
package fsruntime

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record"
	"net/http"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/conversion"
	"strings"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/store"
)

// Options configures a store-backed Cluster / Manager.
type Options struct {
	Scheme *runtime.Scheme
	Store  *store.Store
	Logger logr.Logger
}

// Cluster is a controller-runtime cluster.Cluster backed by Store.
type Cluster struct {
	scheme     *runtime.Scheme
	store      *store.Store
	direct     *fsClient     // store-backed reads/writes (APIReader / projection)
	client     client.Client // cache reads + store writes (reconcile path)
	cache      *fsCache
	restMapper meta.RESTMapper
	logger     logr.Logger
}

var _ cluster.Cluster = (*Cluster)(nil)

// NewCluster builds a Cluster over the given store.
func NewCluster(opts Options) (*Cluster, error) {
	if opts.Scheme == nil {
		return nil, fmt.Errorf("fsruntime: Scheme is required")
	}
	if opts.Store == nil {
		opts.Store = store.New(opts.Scheme)
	}
	if opts.Logger.GetSink() == nil {
		opts.Logger = logr.Discard()
	}

	restMapper := buildRESTMapper(opts.Scheme)
	direct := &fsClient{scheme: opts.Scheme, store: opts.Store, restMapper: restMapper}
	c := &fsCache{
		scheme:     opts.Scheme,
		store:      opts.Store,
		restMapper: restMapper,
		logger:     opts.Logger.WithName("cache"),
		informers:  make(map[schema.GroupVersionKind]toolscache.SharedIndexInformer),
	}
	caching := &cachingClient{cache: c, writer: direct}
	return &Cluster{
		scheme:     opts.Scheme,
		store:      opts.Store,
		direct:     direct,
		client:     caching,
		cache:      c,
		restMapper: restMapper,
		logger:     opts.Logger,
	}, nil
}

// Store returns the underlying object store (useful for tests / providers).
func (c *Cluster) Store() *store.Store { return c.store }

// DirectClient returns a store-backed client that bypasses the informer
// cache. Use this for projecting desired state into the store; reconcilers
// should use GetClient() so they read Reflector-populated cache state.
func (c *Cluster) DirectClient() client.Client { return c.direct }

func (c *Cluster) GetHTTPClient() *http.Client          { return nil }
func (c *Cluster) GetConfig() *rest.Config              { return nil }
func (c *Cluster) GetCache() cache.Cache                { return c.cache }
func (c *Cluster) GetScheme() *runtime.Scheme           { return c.scheme }
func (c *Cluster) GetClient() client.Client             { return c.client }
func (c *Cluster) GetFieldIndexer() client.FieldIndexer { return c.cache }
func (c *Cluster) GetRESTMapper() meta.RESTMapper       { return c.restMapper }
func (c *Cluster) GetAPIReader() client.Reader          { return c.direct }
func (c *Cluster) GetEventRecorderFor(name string) record.EventRecorder {
	return &noopEventRecorder{}
}
func (c *Cluster) GetEventRecorder(name string) events.EventRecorder {
	return &noopEventsRecorder{}
}

// Start starts the cache and blocks until ctx is cancelled.
func (c *Cluster) Start(ctx context.Context) error {
	c.logger.Info("starting fsruntime cluster")
	if err := c.cache.Start(ctx); err != nil {
		return err
	}
	return nil
}

// Manager is a controller-runtime manager.Manager that embeds Cluster.
// Like pgruntime.NewManager, it is a drop-in for ctrl.NewManager when the
// host does not need a real kube-apiserver.
type Manager struct {
	*Cluster

	elected chan struct{}

	mu        sync.Mutex
	runnables []manager.Runnable

	healthzChecks map[string]healthz.Checker
	readyzChecks  map[string]healthz.Checker
}

var _ manager.Manager = (*Manager)(nil)

// NewManager creates a store-backed manager.Manager.
func NewManager(opts Options) (*Manager, error) {
	cl, err := NewCluster(opts)
	if err != nil {
		return nil, err
	}
	elected := make(chan struct{})
	close(elected)
	return &Manager{
		Cluster:       cl,
		elected:       elected,
		healthzChecks: make(map[string]healthz.Checker),
		readyzChecks:  make(map[string]healthz.Checker),
	}, nil
}

func (m *Manager) Add(runnable manager.Runnable) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runnables = append(m.runnables, runnable)
	return nil
}

func (m *Manager) Elected() <-chan struct{} { return m.elected }

func (m *Manager) AddMetricsServerExtraHandler(path string, handler http.Handler) error {
	return nil
}

func (m *Manager) AddHealthzCheck(name string, check healthz.Checker) error {
	m.healthzChecks[name] = check
	return nil
}

func (m *Manager) AddReadyzCheck(name string, check healthz.Checker) error {
	m.readyzChecks[name] = check
	return nil
}

func (m *Manager) Start(ctx context.Context) error {
	m.logger.Info("starting fsruntime manager")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = m.cache.Start(ctx)
	}()

	if !m.cache.WaitForCacheSync(ctx) {
		return fmt.Errorf("fsruntime: cache sync failed")
	}

	m.mu.Lock()
	runnables := append([]manager.Runnable(nil), m.runnables...)
	m.mu.Unlock()

	for _, r := range runnables {
		wg.Add(1)
		go func(r manager.Runnable) {
			defer wg.Done()
			if err := r.Start(ctx); err != nil {
				m.logger.Error(err, "runnable exited with error")
			}
		}(r)
	}

	<-ctx.Done()
	wg.Wait()
	return nil
}

func (m *Manager) GetWebhookServer() webhook.Server {
	panic("fsruntime: webhooks not supported")
}

func (m *Manager) GetLogger() logr.Logger { return m.logger }

func (m *Manager) GetControllerOptions() config.Controller {
	return config.Controller{}
}

func (m *Manager) GetConverterRegistry() conversion.Registry {
	return conversion.NewRegistry()
}

func buildRESTMapper(s *runtime.Scheme) meta.RESTMapper {
	mapper := meta.NewDefaultRESTMapper(s.PrioritizedVersionsAllGroups())
	for gvk := range s.AllKnownTypes() {
		if strings.HasSuffix(gvk.Kind, "List") || gvk.Kind == "" {
			continue
		}
		mapper.Add(gvk, meta.RESTScopeNamespace)
	}
	return mapper
}

type noopEventRecorder struct{}

func (r *noopEventRecorder) Event(object runtime.Object, eventtype, reason, message string) {
}
func (r *noopEventRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
}
func (r *noopEventRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
}

type noopEventsRecorder struct{}

func (r *noopEventsRecorder) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
}
