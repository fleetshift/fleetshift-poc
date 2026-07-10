package kind

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const defaultInventoryDebounce = 50 * time.Millisecond

// InventoryWatcher watches Kubernetes Nodes in kind clusters and
// reports coalesced inventory deltas through a
// [domain.InventoryReporter]. One informer runs per watched cluster;
// pending deltas are keyed by resource name and flushed as a single
// ApplyDeltaBatch to demonstrate Watch + cross-type batching.
//
// Watches are not re-established after a serve restart; they resume
// only when a subsequent deliver succeeds.
type InventoryWatcher struct {
	reporter domain.InventoryReporter

	mu      sync.Mutex
	watches map[string]*clusterWatch // keyed by cluster ResourceName string
	pending map[domain.ResourceName]domain.InventoryDeltaReport

	debounce time.Duration
	now      func() time.Time

	flushTimer *time.Timer
	newClient  func(kubeconfig []byte) (kubernetes.Interface, error)
}

type clusterWatch struct {
	resourceName domain.ResourceName
	stop         chan struct{}
}

// InventoryWatcherOption configures an [InventoryWatcher].
type InventoryWatcherOption func(*InventoryWatcher)

// WithInventoryDebounce sets how long to wait after the last enqueued
// delta before flushing a batch. Tests may set this to zero and call
// FlushForTest.
func WithInventoryDebounce(d time.Duration) InventoryWatcherOption {
	return func(w *InventoryWatcher) { w.debounce = d }
}

// WithInventoryClock overrides the wall clock used for ObservedAt.
func WithInventoryClock(now func() time.Time) InventoryWatcherOption {
	return func(w *InventoryWatcher) { w.now = now }
}

// WithInventoryClientFactory overrides kubeconfig→clientset construction
// (tests inject a fake clientset).
func WithInventoryClientFactory(f func(kubeconfig []byte) (kubernetes.Interface, error)) InventoryWatcherOption {
	return func(w *InventoryWatcher) { w.newClient = f }
}

// NewInventoryWatcher returns a watcher that reports through reporter.
func NewInventoryWatcher(reporter domain.InventoryReporter, opts ...InventoryWatcherOption) *InventoryWatcher {
	w := &InventoryWatcher{
		reporter:  reporter,
		watches:   make(map[string]*clusterWatch),
		pending:   make(map[domain.ResourceName]domain.InventoryDeltaReport),
		debounce:  defaultInventoryDebounce,
		now:       time.Now,
		newClient: clientsetFromKubeconfig,
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

func clientsetFromKubeconfig(kubeconfig []byte) (kubernetes.Interface, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("parse kubeconfig: %w", err)
	}
	return kubernetes.NewForConfig(cfg)
}

// Watch starts a Node informer for the given cluster. kubeconfig must
// reach the cluster API. Calling Watch again for the same name is a
// no-op while a watch is already running.
func (w *InventoryWatcher) Watch(clusterName domain.ResourceName, kubeconfig []byte) error {
	if w == nil {
		return nil
	}
	key := string(clusterName)

	w.mu.Lock()
	if _, ok := w.watches[key]; ok {
		w.mu.Unlock()
		return nil
	}
	w.mu.Unlock()

	client, err := w.newClient(kubeconfig)
	if err != nil {
		return err
	}

	stop := make(chan struct{})
	cw := &clusterWatch{resourceName: clusterName, stop: stop}

	w.mu.Lock()
	if _, ok := w.watches[key]; ok {
		w.mu.Unlock()
		close(stop)
		return nil
	}
	w.watches[key] = cw
	w.mu.Unlock()

	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return client.CoreV1().Nodes().List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			return client.CoreV1().Nodes().Watch(ctx, options)
		},
	}

	informer := cache.NewSharedIndexInformer(lw, &corev1.Node{}, 0, cache.Indexers{})
	_, err = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			node, ok := obj.(*corev1.Node)
			if !ok {
				return
			}
			w.enqueueNode(clusterName, node)
			w.enqueueCluster(clusterName, true)
		},
		UpdateFunc: func(_, newObj any) {
			node, ok := newObj.(*corev1.Node)
			if !ok {
				return
			}
			w.enqueueNode(clusterName, node)
			w.enqueueCluster(clusterName, true)
		},
		DeleteFunc: func(obj any) {
			node, ok := obj.(*corev1.Node)
			if !ok {
				if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
					node, _ = tombstone.Obj.(*corev1.Node)
				}
			}
			if node == nil {
				return
			}
			// Stop reporting this node; resource delete is out of scope
			// (OME-189). Drop any pending delta for the name.
			w.dropPending(nodeResourceName(node.Name))
		},
	})
	if err != nil {
		w.Unwatch(clusterName)
		return fmt.Errorf("add node event handler: %w", err)
	}

	go informer.Run(stop)

	// Initial cluster reachability signal once the informer is up.
	go func() {
		if cache.WaitForCacheSync(stop, informer.HasSynced) {
			w.enqueueCluster(clusterName, true)
		}
	}()

	return nil
}

// Unwatch stops the Node informer for clusterName, if any.
func (w *InventoryWatcher) Unwatch(clusterName domain.ResourceName) {
	if w == nil {
		return
	}
	key := string(clusterName)
	w.mu.Lock()
	cw, ok := w.watches[key]
	if ok {
		delete(w.watches, key)
	}
	w.mu.Unlock()
	if ok {
		close(cw.stop)
		w.enqueueCluster(clusterName, false)
	}
}

// FlushForTest forces an immediate flush of pending deltas. Intended
// for tests that set debounce to zero or need deterministic timing.
func (w *InventoryWatcher) FlushForTest(ctx context.Context) error {
	return w.flush(ctx)
}

func (w *InventoryWatcher) enqueueNode(clusterName domain.ResourceName, node *corev1.Node) {
	report, err := mapNodeDelta(clusterName, node, w.now())
	if err != nil {
		return
	}
	w.enqueue(report)
}

func (w *InventoryWatcher) enqueueCluster(clusterName domain.ResourceName, apiAvailable bool) {
	report, err := mapClusterDelta(clusterName, apiAvailable, w.now())
	if err != nil {
		return
	}
	w.enqueue(report)
}

func (w *InventoryWatcher) enqueue(report domain.InventoryDeltaReport) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pending[report.Name] = report
	if w.debounce <= 0 {
		return
	}
	if w.flushTimer != nil {
		w.flushTimer.Stop()
	}
	w.flushTimer = time.AfterFunc(w.debounce, func() {
		_ = w.flush(context.Background())
	})
}

func (w *InventoryWatcher) dropPending(name domain.ResourceName) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.pending, name)
}

func (w *InventoryWatcher) flush(ctx context.Context) error {
	w.mu.Lock()
	if w.flushTimer != nil {
		w.flushTimer.Stop()
		w.flushTimer = nil
	}
	if len(w.pending) == 0 {
		w.mu.Unlock()
		return nil
	}
	reports := make([]domain.InventoryDeltaReport, 0, len(w.pending))
	for _, r := range w.pending {
		reports = append(reports, r)
	}
	w.pending = make(map[domain.ResourceName]domain.InventoryDeltaReport)
	reporter := w.reporter
	w.mu.Unlock()

	if reporter == nil {
		return nil
	}
	return reporter.ApplyDeltaBatch(ctx, domain.InventoryDeltaBatch{Reports: reports})
}

func nodeResourceName(nodeName string) domain.ResourceName {
	n, err := domain.NewResourceName("nodes", domain.ResourceID(nodeName))
	if err != nil {
		return domain.ResourceName("nodes/" + nodeName)
	}
	return n
}

func mapNodeDelta(clusterName domain.ResourceName, node *corev1.Node, now time.Time) (domain.InventoryDeltaReport, error) {
	labels := map[string]string{}
	for k, v := range node.Labels {
		labels[k] = v
	}
	conds, err := mapKubeNodeConditions(node.Status.Conditions)
	if err != nil {
		return domain.InventoryDeltaReport{}, err
	}
	obs, err := json.Marshal(nodeObservation{
		Cluster:        string(clusterName),
		Addresses:      nodeAddresses(node),
		KubeletVersion: node.Status.NodeInfo.KubeletVersion,
	})
	if err != nil {
		return domain.InventoryDeltaReport{}, err
	}
	raw := json.RawMessage(obs)
	return domain.InventoryDeltaReport{
		ResourceType:      NodeResourceType,
		Name:              nodeResourceName(node.Name),
		ReplaceLabels:     labels,
		ReplaceConditions: conds,
		Observation:       &raw,
		ObservedAt:        now,
	}, nil
}

func mapClusterDelta(clusterName domain.ResourceName, apiAvailable bool, now time.Time) (domain.InventoryDeltaReport, error) {
	status := domain.ConditionFalse
	reason := "APIServerUnavailable"
	message := "cluster API is not reachable"
	if apiAvailable {
		status = domain.ConditionTrue
		reason = "APIServerAvailable"
		message = "cluster API is reachable"
	}
	ready, err := domain.NewCondition("Ready", status, reason, message, now)
	if err != nil {
		return domain.InventoryDeltaReport{}, err
	}
	apiCond, err := domain.NewCondition("APIServerAvailable", status, reason, message, now)
	if err != nil {
		return domain.InventoryDeltaReport{}, err
	}
	obs, err := json.Marshal(clusterObservation{APIServerAvailable: apiAvailable})
	if err != nil {
		return domain.InventoryDeltaReport{}, err
	}
	raw := json.RawMessage(obs)
	return domain.InventoryDeltaReport{
		ResourceType:      ClusterResourceType,
		Name:              clusterName,
		ReplaceConditions: []domain.Condition{ready, apiCond},
		Observation:       &raw,
		ObservedAt:        now,
	}, nil
}

type nodeObservation struct {
	Cluster        string            `json:"cluster"`
	Addresses      map[string]string `json:"addresses,omitempty"`
	KubeletVersion string            `json:"kubeletVersion,omitempty"`
}

type clusterObservation struct {
	APIServerAvailable bool `json:"apiServerAvailable"`
}

func nodeAddresses(node *corev1.Node) map[string]string {
	out := make(map[string]string, len(node.Status.Addresses))
	for _, a := range node.Status.Addresses {
		out[string(a.Type)] = a.Address
	}
	return out
}

func mapKubeNodeConditions(in []corev1.NodeCondition) ([]domain.Condition, error) {
	out := make([]domain.Condition, 0, len(in))
	for _, c := range in {
		status := domain.ConditionUnknown
		switch c.Status {
		case corev1.ConditionTrue:
			status = domain.ConditionTrue
		case corev1.ConditionFalse:
			status = domain.ConditionFalse
		}
		cond, err := domain.NewCondition(domain.ConditionType(c.Type), status, c.Reason, c.Message, c.LastTransitionTime.Time)
		if err != nil {
			return nil, err
		}
		out = append(out, cond)
	}
	return out, nil
}
