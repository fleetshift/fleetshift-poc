package kubernetes

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type fakeTargetLister struct {
	mu      sync.Mutex
	targets []domain.TargetInfo
	err     error
}

func (l *fakeTargetLister) ListTargets(context.Context) ([]domain.TargetInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return nil, l.err
	}
	out := make([]domain.TargetInfo, len(l.targets))
	copy(out, l.targets)
	return out, nil
}

func (l *fakeTargetLister) set(targets ...domain.TargetInfo) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.targets = append([]domain.TargetInfo(nil), targets...)
}

type runtimeCall struct {
	op     string
	target domain.TargetID
}

type fakeIndexRuntime struct {
	mu              sync.Mutex
	calls           []runtimeCall
	running         map[domain.TargetID]domain.TargetInfo
	startErr        error
	stopErr         error
	stopAllErr      error
	stopAllCount    int
	lastStopTimeout time.Duration // 0 if StopIndexer ctx had no deadline
	startedCh       chan struct{}
	stoppedCh       chan struct{}
}

func newFakeIndexRuntime() *fakeIndexRuntime {
	return &fakeIndexRuntime{
		running:   make(map[domain.TargetID]domain.TargetInfo),
		startedCh: make(chan struct{}, 16),
		stoppedCh: make(chan struct{}, 16),
	}
}

func (r *fakeIndexRuntime) StartIndexer(_ context.Context, target domain.TargetInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, runtimeCall{op: "start", target: target.ID()})
	if r.startErr != nil {
		return r.startErr
	}
	if _, ok := r.running[target.ID()]; ok {
		return nil
	}
	r.running[target.ID()] = target
	select {
	case r.startedCh <- struct{}{}:
	default:
	}
	return nil
}

func (r *fakeIndexRuntime) StopIndexer(ctx context.Context, target domain.TargetInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, runtimeCall{op: "stop", target: target.ID()})
	if dl, ok := ctx.Deadline(); ok {
		r.lastStopTimeout = time.Until(dl)
	} else {
		r.lastStopTimeout = 0
	}
	if r.stopErr != nil {
		return r.stopErr
	}
	delete(r.running, target.ID())
	select {
	case r.stoppedCh <- struct{}{}:
	default:
	}
	return nil
}

func (r *fakeIndexRuntime) StopAllIndexers(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, runtimeCall{op: "stopAllIndexers"})
	r.stopAllCount++
	r.running = make(map[domain.TargetID]domain.TargetInfo)
	return r.stopAllErr
}

func (r *fakeIndexRuntime) HasIndexer(id domain.TargetID) bool {
	return r.isRunning(id)
}

func (r *fakeIndexRuntime) isRunning(id domain.TargetID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.running[id]
	return ok
}

func (r *fakeIndexRuntime) callOps() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	for i, c := range r.calls {
		if c.target == "" {
			out[i] = c.op
		} else {
			out[i] = c.op + ":" + string(c.target)
		}
	}
	return out
}

type indexHostTestVault struct {
	secrets map[domain.SecretRef][]byte
}

func (v *indexHostTestVault) Get(_ context.Context, ref domain.SecretRef) ([]byte, error) {
	val, ok := v.secrets[ref]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return val, nil
}

func (v *indexHostTestVault) Put(_ context.Context, ref domain.SecretRef, val []byte) error {
	v.secrets[ref] = val
	return nil
}

func (v *indexHostTestVault) Delete(_ context.Context, ref domain.SecretRef) error {
	delete(v.secrets, ref)
	return nil
}

func readyKubeTarget(id string, props map[string]string) domain.TargetInfo {
	if props == nil {
		props = map[string]string{
			PropAPIServer:           "https://127.0.0.1:6443",
			PropServiceAccountToken: "token",
		}
	}
	return domain.NewTargetInfo(
		domain.TargetID(id),
		TargetType,
		"Test Cluster",
		domain.TargetStateReady,
		nil,
		props,
		nil,
	)
}

func startController(t *testing.T, lister TargetLister, runtime InProcessIndexRuntime, opts ...InProcessIndexControllerOption) (context.CancelFunc, *InProcessIndexController) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ctrl := NewInProcessIndexController(
		lister,
		runtime,
		DefaultInProcessIndexPolicy{},
		slog.New(slog.DiscardHandler),
		opts...,
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctrl.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return cancel, ctrl
}

func waitFor(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", msg)
	}
}

func TestDefaultInProcessIndexPolicy_DesiredMatrix(t *testing.T) {
	policy := DefaultInProcessIndexPolicy{}

	tests := []struct {
		name   string
		target domain.TargetInfo
		wantOK bool
	}{
		{
			name:   "ready kubernetes local default",
			target: readyKubeTarget("t1", nil),
			wantOK: true,
		},
		{
			name: "empty state treated as ready",
			target: domain.NewTargetInfo("t1", TargetType, "n", "", nil, map[string]string{
				PropAPIServer: "https://example",
			}, nil),
			wantOK: true,
		},
		{
			name:   "non-kubernetes ignored",
			target: domain.NewTargetInfo("t1", "kind", "n", domain.TargetStateReady, nil, nil, nil),
			wantOK: false,
		},
		{
			name: "non-ready ignored",
			target: domain.NewTargetInfo("t1", TargetType, "n", domain.TargetStateInitializing, nil, map[string]string{
				PropAPIServer: "https://example",
			}, nil),
			wantOK: false,
		},
		{
			name: "external mode ignored",
			target: readyKubeTarget("t1", map[string]string{
				PropAPIServer:     "https://example",
				PropInventoryMode: string(InventoryModeExternal),
			}),
			wantOK: false,
		},
		{
			name: "disabled mode ignored",
			target: readyKubeTarget("t1", map[string]string{
				PropAPIServer:     "https://example",
				PropInventoryMode: string(InventoryModeDisabled),
			}),
			wantOK: false,
		},
		{
			name: "explicit local mode desired",
			target: readyKubeTarget("t1", map[string]string{
				PropAPIServer:     "https://example",
				PropInventoryMode: string(InventoryModeInProcess),
			}),
			wantOK: true,
		},
		{
			name: "draining ignored",
			target: domain.NewTargetInfo("t1", TargetType, "n", domain.TargetStateDraining, nil, map[string]string{
				PropAPIServer: "https://example",
			}, nil),
			wantOK: false,
		},
		{
			name: "terminated ignored",
			target: domain.NewTargetInfo("t1", TargetType, "n", domain.TargetStateTerminated, nil, map[string]string{
				PropAPIServer: "https://example",
			}, nil),
			wantOK: false,
		},
		{
			name: "unknown mode ignored",
			target: readyKubeTarget("t1", map[string]string{
				PropAPIServer:     "https://example",
				PropInventoryMode: "foo",
			}),
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision, ok := policy.Desired(tt.target)
			if ok != tt.wantOK {
				t.Fatalf("Desired ok = %v, want %v", ok, tt.wantOK)
			}
			if tt.wantOK && decision.Fingerprint == "" {
				t.Fatal("expected non-empty fingerprint for desired target")
			}
		})
	}
}

func TestInProcessIndexController_StartupReconcileStartsReadyLocalTargets(t *testing.T) {
	lister := &fakeTargetLister{}
	lister.set(readyKubeTarget("t1", nil))
	runtime := newFakeIndexRuntime()

	_, _ = startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	if !runtime.isRunning("t1") {
		t.Fatal("expected t1 to be running after startup reconcile")
	}
}

func TestInProcessIndexController_IgnoresNonKubernetesAndNonReadyAndNonLocal(t *testing.T) {
	lister := &fakeTargetLister{}
	lister.set(
		domain.NewTargetInfo("kind-1", "kind", "n", domain.TargetStateReady, nil, nil, nil),
		domain.NewTargetInfo("init-1", TargetType, "n", domain.TargetStateInitializing, nil, map[string]string{
			PropAPIServer: "https://example",
		}, nil),
		readyKubeTarget("ext-1", map[string]string{
			PropAPIServer:     "https://example",
			PropInventoryMode: string(InventoryModeExternal),
		}),
		readyKubeTarget("dis-1", map[string]string{
			PropAPIServer:     "https://example",
			PropInventoryMode: string(InventoryModeDisabled),
		}),
	)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	ctrl.NotifyTargetReady(context.Background(), readyKubeTarget("ext-1", nil))
	time.Sleep(20 * time.Millisecond)

	if ops := runtime.callOps(); len(ops) != 0 {
		t.Fatalf("expected no runtime calls, got %v", ops)
	}
}

func TestInProcessIndexController_StopsNoLongerDesired(t *testing.T) {
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", nil)
	lister.set(target)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	lister.set()
	ctrl.NotifyTargetReady(context.Background(), target)
	waitFor(t, runtime.stoppedCh, "stop")

	if runtime.isRunning("t1") {
		t.Fatal("expected t1 to be stopped after leaving desired set")
	}
}

func TestInProcessIndexController_StopsWhenModeFlipsToExternalWhileStillListed(t *testing.T) {
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", map[string]string{
		PropAPIServer:           "https://127.0.0.1:6443",
		PropServiceAccountToken: "token",
	})
	lister.set(target)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	external := readyKubeTarget("t1", map[string]string{
		PropAPIServer:           "https://127.0.0.1:6443",
		PropServiceAccountToken: "token",
		PropInventoryMode:       string(InventoryModeExternal),
	})
	lister.set(external)
	ctrl.NotifyTargetReady(context.Background(), external)
	waitFor(t, runtime.stoppedCh, "stop after mode flip")

	if runtime.isRunning("t1") {
		t.Fatal("expected t1 stopped after mode flipped to external while still listed")
	}
	select {
	case <-runtime.startedCh:
		t.Fatal("external-mode target must not restart while still listed")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestInProcessIndexController_StopsWhenStateFlipsToDrainingWhileStillListed(t *testing.T) {
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", nil)
	lister.set(target)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	draining := domain.NewTargetInfo(
		"t1", TargetType, "Test Cluster", domain.TargetStateDraining, nil,
		map[string]string{
			PropAPIServer:           "https://127.0.0.1:6443",
			PropServiceAccountToken: "token",
		}, nil,
	)
	lister.set(draining)
	ctrl.NotifyTargetReady(context.Background(), draining)
	waitFor(t, runtime.stoppedCh, "stop after draining")

	if runtime.isRunning("t1") {
		t.Fatal("expected t1 stopped after state flipped to draining while still listed")
	}
}

func TestInProcessIndexController_MultiTargetStopsOnlyUndesired(t *testing.T) {
	lister := &fakeTargetLister{}
	a := readyKubeTarget("a", nil)
	b := readyKubeTarget("b", nil)
	lister.set(a, b)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start a or b")
	waitFor(t, runtime.startedCh, "start remaining")

	if !runtime.isRunning("a") || !runtime.isRunning("b") {
		t.Fatalf("expected both a and b running, a=%v b=%v", runtime.isRunning("a"), runtime.isRunning("b"))
	}

	// Flip only A to external; B stays desired and listed.
	aExternal := readyKubeTarget("a", map[string]string{
		PropAPIServer:           "https://127.0.0.1:6443",
		PropServiceAccountToken: "token",
		PropInventoryMode:       string(InventoryModeExternal),
	})
	lister.set(aExternal, b)
	ctrl.NotifyTargetReady(context.Background(), aExternal)
	waitFor(t, runtime.stoppedCh, "stop a")

	if runtime.isRunning("a") {
		t.Fatal("expected a stopped")
	}
	if !runtime.isRunning("b") {
		t.Fatal("expected b to keep running")
	}
}

func TestInProcessIndexController_FingerprintChangeRestarts(t *testing.T) {
	lister := &fakeTargetLister{}
	props := map[string]string{
		PropAPIServer:           "https://127.0.0.1:6443",
		PropServiceAccountToken: "token-1",
	}
	lister.set(readyKubeTarget("t1", props))
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	props[PropServiceAccountToken] = "token-2"
	lister.set(readyKubeTarget("t1", props))
	ctrl.NotifyTargetReady(context.Background(), readyKubeTarget("t1", props))
	waitFor(t, runtime.stoppedCh, "stop before restart")
	waitFor(t, runtime.startedCh, "restart start")

	ops := runtime.callOps()
	if len(ops) < 3 || ops[0] != "start:t1" || ops[1] != "stop:t1" || ops[2] != "start:t1" {
		t.Fatalf("expected start/stop/start sequence, got %v", ops)
	}
}

func TestInProcessIndexController_LostNotificationsRecoveredByPeriodicReconcile(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lister := &fakeTargetLister{}
		runtime := newFakeIndexRuntime()
		_, _ = startController(t, lister, runtime, WithReconcileInterval(time.Second))

		synctest.Wait()

		lister.set(readyKubeTarget("late", nil))
		time.Sleep(time.Second)
		synctest.Wait()

		if !runtime.isRunning("late") {
			t.Fatal("expected periodic reconcile to start target after missed notification")
		}
	})
}

func TestInProcessIndexController_RestartsAfterUnexpectedExit(t *testing.T) {
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", nil)
	lister.set(target)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	runtime.mu.Lock()
	delete(runtime.running, "t1")
	runtime.mu.Unlock()

	ctrl.NotifyTargetReady(context.Background(), target)
	waitFor(t, runtime.startedCh, "restart after unexpected exit")

	if !runtime.isRunning("t1") {
		t.Fatal("expected t1 to be restarted after unexpected exit")
	}
	starts := 0
	for _, op := range runtime.callOps() {
		if op == "start:t1" {
			starts++
		}
	}
	if starts != 2 {
		t.Fatalf("starts = %d, want 2 (initial + restart)", starts)
	}
}

func TestInProcessIndexController_DuplicateNotificationsHarmless(t *testing.T) {
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", nil)
	lister.set(target)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	ctrl.NotifyTargetReady(context.Background(), target)
	ctrl.NotifyTargetReady(context.Background(), target)
	ctrl.NotifyTargetReady(context.Background(), target)
	time.Sleep(20 * time.Millisecond)

	starts := 0
	for _, op := range runtime.callOps() {
		if op == "start:t1" {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("duplicate ready notifications caused %d starts, want 1", starts)
	}
}

func TestInProcessIndexController_TerminatingNotificationBoundedStop(t *testing.T) {
	// Target stays listed: stop must come from the terminating hint itself,
	// not from the "no longer listed" undesired path.
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", nil)
	lister.set(target)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	ctrl.NotifyTargetTerminating(context.Background(), target)
	waitFor(t, runtime.stoppedCh, "terminating stop")

	if runtime.isRunning("t1") {
		t.Fatal("expected terminating notification to stop the in-process indexer")
	}
}

func TestInProcessIndexController_TerminatingTargetDoesNotRestartWhileStillListed(t *testing.T) {
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", nil)
	lister.set(target)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	ctrl.NotifyTargetTerminating(context.Background(), target)
	waitFor(t, runtime.stoppedCh, "terminating stop")

	select {
	case <-runtime.startedCh:
		t.Fatal("terminating target restarted while still listed")
	case <-time.After(50 * time.Millisecond):
	}

	starts := 0
	for _, op := range runtime.callOps() {
		if op == "start:t1" {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("starts = %d, want 1", starts)
	}
}

func TestInProcessIndexController_ReadyNotificationClearsTerminatingSuppression(t *testing.T) {
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", nil)
	lister.set(target)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	ctrl.NotifyTargetTerminating(context.Background(), target)
	waitFor(t, runtime.stoppedCh, "terminating stop")

	ctrl.NotifyTargetReady(context.Background(), target)
	waitFor(t, runtime.startedCh, "restart after ready")

	starts := 0
	for _, op := range runtime.callOps() {
		if op == "start:t1" {
			starts++
		}
	}
	if starts != 2 {
		t.Fatalf("starts = %d, want 2", starts)
	}
}

func TestInProcessIndexController_ShutdownCallsStopAllIndexers(t *testing.T) {
	lister := &fakeTargetLister{}
	lister.set(readyKubeTarget("t1", nil))
	runtime := newFakeIndexRuntime()

	cancel, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	cancel()
	deadline := time.After(2 * time.Second)
	for {
		runtime.mu.Lock()
		n := runtime.stopAllCount
		runtime.mu.Unlock()
		ctrl.mu.Lock()
		runningN, termN := len(ctrl.running), len(ctrl.terminating)
		ctrl.mu.Unlock()
		if n >= 1 && runningN == 0 && termN == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for shutdown; stopAll=%d running=%d terminating=%d",
				n, runningN, termN)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestInProcessIndexController_ShutdownStopAllFailureStillClearsState(t *testing.T) {
	lister := &fakeTargetLister{}
	lister.set(readyKubeTarget("t1", nil))
	runtime := newFakeIndexRuntime()
	runtime.stopAllErr = errors.New("stop all failed")

	cancel, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")
	cancel()
	deadline := time.After(2 * time.Second)
	for {
		runtime.mu.Lock()
		n := runtime.stopAllCount
		runtime.mu.Unlock()
		ctrl.mu.Lock()
		runningN, termN := len(ctrl.running), len(ctrl.terminating)
		ctrl.mu.Unlock()
		if n >= 1 && runningN == 0 && termN == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for shutdown after StopAll failure; stopAll=%d running=%d terminating=%d",
				n, runningN, termN)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestKubernetesInProcessIndexHost_StartStopIdempotent(t *testing.T) {
	host := NewKubernetesInProcessIndexHost(
		context.Background(),
		nil,
		&recordingReporter{},
		slog.New(slog.DiscardHandler),
		WithInProcessIndexHostRESTConfigFactory(func(context.Context, domain.TargetInfo) (*rest.Config, error) {
			return &rest.Config{Host: "https://example"}, nil
		}),
		WithInProcessIndexHostDynamicClientFactory(func(*rest.Config) (dynamic.Interface, error) {
			return newFakeDynamicClient(podsGVR(), crdGVR), nil
		}),
		WithInProcessIndexHostDiscoveryClientFactory(func(*rest.Config) (discovery.DiscoveryInterface, error) {
			return newFakeDiscovery([]*metav1.APIResourceList{{
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{
					{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
				},
			}}), nil
		}),
		WithInProcessIndexHostIndexConfig(func(domain.TargetInfo) IndexConfig {
			return IndexConfig{BatchInterval: time.Hour}
		}),
	)

	target := readyKubeTarget("host-1", nil)
	if err := host.StartIndexer(context.Background(), target); err != nil {
		t.Fatalf("StartIndexer: %v", err)
	}
	if err := host.StartIndexer(context.Background(), target); err != nil {
		t.Fatalf("idempotent StartIndexer: %v", err)
	}
	if !host.Running("host-1") {
		t.Fatal("expected host-1 running")
	}

	if err := host.StopIndexer(context.Background(), target); err != nil {
		t.Fatalf("StopIndexer: %v", err)
	}
	if err := host.StopIndexer(context.Background(), target); err != nil {
		t.Fatalf("idempotent StopIndexer: %v", err)
	}
	if host.Running("host-1") {
		t.Fatal("expected host-1 stopped")
	}

	if err := host.StopAllIndexers(context.Background()); err != nil {
		t.Fatalf("StopAllIndexers: %v", err)
	}
}

func TestBuildTargetRESTConfigFromVault(t *testing.T) {
	vault := &indexHostTestVault{secrets: map[domain.SecretRef][]byte{
		"targets/t1/sa-token": []byte("vault-token"),
	}}

	cfg, err := BuildTargetRESTConfig(context.Background(), vault, readyKubeTarget("t1", map[string]string{
		PropAPIServer:              "https://cluster.example:6443",
		PropCACert:                 "ca-data",
		PropServiceAccountTokenRef: "targets/t1/sa-token",
	}))
	if err != nil {
		t.Fatalf("BuildTargetRESTConfig: %v", err)
	}
	if cfg.Host != "https://cluster.example:6443" {
		t.Fatalf("Host = %q", cfg.Host)
	}
	if cfg.BearerToken != "vault-token" {
		t.Fatalf("BearerToken = %q, want vault-token", cfg.BearerToken)
	}
	if string(cfg.TLSClientConfig.CAData) != "ca-data" {
		t.Fatalf("CAData = %q", cfg.TLSClientConfig.CAData)
	}
}

func TestKubernetesInProcessIndexHost_RejectsNonKubernetesTarget(t *testing.T) {
	host := NewKubernetesInProcessIndexHost(
		context.Background(),
		nil,
		&recordingReporter{},
		slog.New(slog.DiscardHandler),
	)
	err := host.StartIndexer(context.Background(), domain.NewTargetInfo(
		"x", "kind", "n", domain.TargetStateReady, nil, nil, nil,
	))
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("StartIndexer error = %v, want ErrInvalidArgument", err)
	}
}

func TestInProcessIndexController_NilLoggerAndOptionDefaults(t *testing.T) {
	lister := &fakeTargetLister{}
	runtime := newFakeIndexRuntime()
	ctrl := NewInProcessIndexController(lister, runtime, DefaultInProcessIndexPolicy{}, nil,
		WithReconcileInterval(0),
		WithStopTimeout(0),
		WithReconcileInterval(time.Hour),
		WithStopTimeout(150*time.Millisecond),
	)
	if ctrl.logger == nil {
		t.Fatal("expected default logger when nil is passed")
	}
	if ctrl.reconcileEvery != time.Hour {
		t.Fatalf("reconcileEvery = %v, want 1h", ctrl.reconcileEvery)
	}
	if ctrl.stopTimeout != 150*time.Millisecond {
		t.Fatalf("stopTimeout = %v, want 150ms", ctrl.stopTimeout)
	}
}

func TestInProcessIndexController_WithStopTimeoutAppliedOnTerminating(t *testing.T) {
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", nil)
	lister.set(target)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime,
		WithReconcileInterval(time.Hour),
		WithStopTimeout(200*time.Millisecond),
	)
	waitFor(t, runtime.startedCh, "start")

	ctrl.NotifyTargetTerminating(context.Background(), target)
	waitFor(t, runtime.stoppedCh, "terminating stop")

	runtime.mu.Lock()
	got := runtime.lastStopTimeout
	runtime.mu.Unlock()
	if got < 100*time.Millisecond || got > 200*time.Millisecond {
		t.Fatalf("lastStopTimeout = %v, want roughly 200ms", got)
	}
}

func TestInProcessIndexController_WithStopTimeoutAppliedOnReconcileStop(t *testing.T) {
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", nil)
	lister.set(target)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime,
		WithReconcileInterval(time.Hour),
		WithStopTimeout(200*time.Millisecond),
	)
	waitFor(t, runtime.startedCh, "start")

	lister.set()
	ctrl.NotifyTargetReady(context.Background(), target)
	waitFor(t, runtime.stoppedCh, "undesired stop")

	runtime.mu.Lock()
	got := runtime.lastStopTimeout
	runtime.mu.Unlock()
	if got < 100*time.Millisecond || got > 200*time.Millisecond {
		t.Fatalf("reconcile stop lastStopTimeout = %v, want roughly 200ms", got)
	}
}

func TestInProcessIndexController_ListTargetsErrorSkipsReconcile(t *testing.T) {
	lister := &fakeTargetLister{err: errors.New("store unavailable")}
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	time.Sleep(30 * time.Millisecond)
	if ops := runtime.callOps(); len(ops) != 0 {
		t.Fatalf("expected no runtime calls while list fails, got %v", ops)
	}

	lister.mu.Lock()
	lister.err = nil
	lister.targets = []domain.TargetInfo{readyKubeTarget("t1", nil)}
	lister.mu.Unlock()
	ctrl.NotifyTargetReady(context.Background(), readyKubeTarget("t1", nil))
	waitFor(t, runtime.startedCh, "start after list recovers")
}

func TestInProcessIndexController_StartIndexerFailureRetries(t *testing.T) {
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", nil)
	lister.set(target)
	runtime := newFakeIndexRuntime()
	runtime.startErr = errors.New("start failed")

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	time.Sleep(30 * time.Millisecond)
	if runtime.isRunning("t1") {
		t.Fatal("expected start failure to leave target not running")
	}

	runtime.mu.Lock()
	runtime.startErr = nil
	runtime.mu.Unlock()
	ctrl.NotifyTargetReady(context.Background(), target)
	waitFor(t, runtime.startedCh, "retry start")
	if !runtime.isRunning("t1") {
		t.Fatal("expected retry to start indexer")
	}
}

func TestInProcessIndexController_StopUndesiredFailureRetainsRunning(t *testing.T) {
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", nil)
	lister.set(target)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	runtime.mu.Lock()
	runtime.stopErr = errors.New("stop failed")
	runtime.mu.Unlock()
	lister.set()
	ctrl.NotifyTargetReady(context.Background(), target)
	time.Sleep(30 * time.Millisecond)
	if !runtime.isRunning("t1") {
		t.Fatal("expected failed stop to leave runtime indexer running")
	}

	runtime.mu.Lock()
	runtime.stopErr = nil
	runtime.mu.Unlock()
	ctrl.NotifyTargetReady(context.Background(), target)
	waitFor(t, runtime.stoppedCh, "retry stop")
	if runtime.isRunning("t1") {
		t.Fatal("expected retry stop to remove indexer")
	}
}

func TestInProcessIndexController_FingerprintRestartStopFailureSkipsStart(t *testing.T) {
	lister := &fakeTargetLister{}
	props := map[string]string{
		PropAPIServer:           "https://127.0.0.1:6443",
		PropServiceAccountToken: "token-1",
	}
	lister.set(readyKubeTarget("t1", props))
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	runtime.mu.Lock()
	runtime.stopErr = errors.New("stop failed")
	runtime.mu.Unlock()
	props[PropServiceAccountToken] = "token-2"
	lister.set(readyKubeTarget("t1", props))
	ctrl.NotifyTargetReady(context.Background(), readyKubeTarget("t1", props))
	time.Sleep(30 * time.Millisecond)

	starts := 0
	for _, op := range runtime.callOps() {
		if op == "start:t1" {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("starts = %d, want 1 after failed fingerprint stop", starts)
	}
}

func TestInProcessIndexController_TerminatingStopFailureStillSuppressesRestart(t *testing.T) {
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", nil)
	lister.set(target)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	runtime.mu.Lock()
	runtime.stopErr = errors.New("bounded stop failed")
	runtime.mu.Unlock()
	ctrl.NotifyTargetTerminating(context.Background(), target)
	time.Sleep(30 * time.Millisecond)

	select {
	case <-runtime.startedCh:
		t.Fatal("terminating target restarted after stop failure")
	case <-time.After(50 * time.Millisecond):
	}

	starts := 0
	for _, op := range runtime.callOps() {
		if op == "start:t1" {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("starts = %d, want 1", starts)
	}
}

func TestInProcessIndexController_TerminatingClearedWhenTargetLeavesList(t *testing.T) {
	lister := &fakeTargetLister{}
	target := readyKubeTarget("t1", nil)
	lister.set(target)
	runtime := newFakeIndexRuntime()

	_, ctrl := startController(t, lister, runtime, WithReconcileInterval(time.Hour))
	waitFor(t, runtime.startedCh, "start")

	ctrl.NotifyTargetTerminating(context.Background(), target)
	waitFor(t, runtime.stoppedCh, "terminating stop")

	lister.set()
	other := readyKubeTarget("other", nil)
	ctrl.NotifyTargetTerminating(context.Background(), other)
	time.Sleep(30 * time.Millisecond)

	lister.set(target)
	ctrl.NotifyTargetTerminating(context.Background(), other)
	waitFor(t, runtime.startedCh, "restart after terminating cleared")

	starts := 0
	for _, op := range runtime.callOps() {
		if op == "start:t1" {
			starts++
		}
	}
	if starts != 2 {
		t.Fatalf("starts = %d, want 2", starts)
	}
}

func TestBuildTargetRESTConfigErrorsAndDirectToken(t *testing.T) {
	if _, err := BuildTargetRESTConfig(context.Background(), nil, readyKubeTarget("t1", map[string]string{})); err == nil {
		t.Fatal("expected missing api_server error")
	}

	cfg, err := BuildTargetRESTConfig(context.Background(), nil, readyKubeTarget("t1", map[string]string{
		PropAPIServer:              "https://direct.example",
		PropServiceAccountToken:    "direct-token",
		PropServiceAccountTokenRef: "targets/t1/sa-token",
	}))
	if err != nil {
		t.Fatalf("direct token BuildTargetRESTConfig: %v", err)
	}
	if cfg.BearerToken != "direct-token" {
		t.Fatalf("BearerToken = %q, want direct-token (direct wins over vault ref)", cfg.BearerToken)
	}

	// API server alone is accepted: bearer token may be absent (e.g. test
	// clusters using other auth). Callers that need a token must set one.
	cfg, err = BuildTargetRESTConfig(context.Background(), nil, readyKubeTarget("t1", map[string]string{
		PropAPIServer: "https://no-creds.example",
	}))
	if err != nil {
		t.Fatalf("api_server-only BuildTargetRESTConfig: %v", err)
	}
	if cfg.Host != "https://no-creds.example" {
		t.Fatalf("Host = %q", cfg.Host)
	}
	if cfg.BearerToken != "" {
		t.Fatalf("BearerToken = %q, want empty when no token/ref set", cfg.BearerToken)
	}

	if _, err := BuildTargetRESTConfig(context.Background(), nil, readyKubeTarget("t1", map[string]string{
		PropAPIServer:              "https://cluster.example",
		PropServiceAccountTokenRef: "targets/t1/sa-token",
	})); err == nil {
		t.Fatal("expected vault-required error when vault is nil")
	}

	vault := &indexHostTestVault{secrets: map[domain.SecretRef][]byte{}}
	if _, err := BuildTargetRESTConfig(context.Background(), vault, readyKubeTarget("t1", map[string]string{
		PropAPIServer:              "https://cluster.example",
		PropServiceAccountTokenRef: "missing-ref",
	})); err == nil {
		t.Fatal("expected vault get error for missing ref")
	}
}

func TestKubernetesInProcessIndexHost_StartIndexerClientErrors(t *testing.T) {
	target := readyKubeTarget("t1", nil)

	t.Run("rest config", func(t *testing.T) {
		host := NewKubernetesInProcessIndexHost(
			context.Background(), nil, &recordingReporter{}, slog.New(slog.DiscardHandler),
			WithInProcessIndexHostRESTConfigFactory(func(context.Context, domain.TargetInfo) (*rest.Config, error) {
				return nil, errors.New("bad config")
			}),
		)
		if err := host.StartIndexer(context.Background(), target); err == nil {
			t.Fatal("expected rest config error")
		}
	})

	t.Run("dynamic client", func(t *testing.T) {
		host := NewKubernetesInProcessIndexHost(
			context.Background(), nil, &recordingReporter{}, slog.New(slog.DiscardHandler),
			WithInProcessIndexHostRESTConfigFactory(func(context.Context, domain.TargetInfo) (*rest.Config, error) {
				return &rest.Config{Host: "https://example"}, nil
			}),
			WithInProcessIndexHostDynamicClientFactory(func(*rest.Config) (dynamic.Interface, error) {
				return nil, errors.New("dynamic failed")
			}),
		)
		if err := host.StartIndexer(context.Background(), target); err == nil {
			t.Fatal("expected dynamic client error")
		}
	})

	t.Run("discovery client", func(t *testing.T) {
		host := NewKubernetesInProcessIndexHost(
			context.Background(), nil, &recordingReporter{}, slog.New(slog.DiscardHandler),
			WithInProcessIndexHostRESTConfigFactory(func(context.Context, domain.TargetInfo) (*rest.Config, error) {
				return &rest.Config{Host: "https://example"}, nil
			}),
			WithInProcessIndexHostDynamicClientFactory(func(*rest.Config) (dynamic.Interface, error) {
				return newFakeDynamicClient(podsGVR()), nil
			}),
			WithInProcessIndexHostDiscoveryClientFactory(func(*rest.Config) (discovery.DiscoveryInterface, error) {
				return nil, errors.New("discovery failed")
			}),
		)
		if err := host.StartIndexer(context.Background(), target); err == nil {
			t.Fatal("expected discovery client error")
		}
	})
}

// blockingDiscovery hangs in ServerPreferredResources until unblock is closed,
// so the in-process indexer goroutine does not close done until then.
type blockingDiscovery struct {
	*fakeDiscoveryWithPreferred
	unblock <-chan struct{}
}

func (d *blockingDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	<-d.unblock
	return d.fakeDiscoveryWithPreferred.ServerPreferredResources()
}

func newHostWithBlockingDiscovery(t *testing.T, unblock <-chan struct{}) *KubernetesInProcessIndexHost {
	t.Helper()
	return NewKubernetesInProcessIndexHost(
		context.Background(),
		nil,
		&recordingReporter{},
		slog.New(slog.DiscardHandler),
		WithInProcessIndexHostRESTConfigFactory(func(context.Context, domain.TargetInfo) (*rest.Config, error) {
			return &rest.Config{Host: "https://example"}, nil
		}),
		WithInProcessIndexHostDynamicClientFactory(func(*rest.Config) (dynamic.Interface, error) {
			return newFakeDynamicClient(podsGVR()), nil
		}),
		WithInProcessIndexHostDiscoveryClientFactory(func(*rest.Config) (discovery.DiscoveryInterface, error) {
			return &blockingDiscovery{
				fakeDiscoveryWithPreferred: newFakeDiscovery([]*metav1.APIResourceList{{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
					},
				}}),
				unblock: unblock,
			}, nil
		}),
		WithInProcessIndexHostIndexConfig(func(domain.TargetInfo) IndexConfig {
			return IndexConfig{BatchInterval: time.Hour}
		}),
	)
}

func TestKubernetesInProcessIndexHost_StopIndexerTimeout(t *testing.T) {
	unblock := make(chan struct{})
	host := newHostWithBlockingDiscovery(t, unblock)
	target := readyKubeTarget("host-timeout", nil)

	if err := host.StartIndexer(context.Background(), target); err != nil {
		t.Fatalf("StartIndexer: %v", err)
	}
	if !host.Running("host-timeout") {
		t.Fatal("expected indexer tracked while blocked in discovery")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := host.StopIndexer(stopCtx, target)
	if err == nil {
		t.Fatal("expected stop timeout error")
	}
	if !host.Running("host-timeout") {
		t.Fatal("expected indexer left tracked after stop timeout")
	}

	close(unblock)
	deadline := time.After(2 * time.Second)
	for host.Running("host-timeout") {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for background stop cleanup")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestKubernetesInProcessIndexHost_StopAllIndexersWithRunningAndTimeout(t *testing.T) {
	t.Run("stops running indexer", func(t *testing.T) {
		host := NewKubernetesInProcessIndexHost(
			context.Background(),
			nil,
			&recordingReporter{},
			slog.New(slog.DiscardHandler),
			WithInProcessIndexHostRESTConfigFactory(func(context.Context, domain.TargetInfo) (*rest.Config, error) {
				return &rest.Config{Host: "https://example"}, nil
			}),
			WithInProcessIndexHostDynamicClientFactory(func(*rest.Config) (dynamic.Interface, error) {
				return newFakeDynamicClient(podsGVR(), crdGVR), nil
			}),
			WithInProcessIndexHostDiscoveryClientFactory(func(*rest.Config) (discovery.DiscoveryInterface, error) {
				return newFakeDiscovery([]*metav1.APIResourceList{{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
					},
				}}), nil
			}),
			WithInProcessIndexHostIndexConfig(func(domain.TargetInfo) IndexConfig {
				return IndexConfig{BatchInterval: time.Hour}
			}),
		)
		target := readyKubeTarget("host-all", nil)
		if err := host.StartIndexer(context.Background(), target); err != nil {
			t.Fatalf("StartIndexer: %v", err)
		}
		if err := host.StopAllIndexers(context.Background()); err != nil {
			t.Fatalf("StopAllIndexers: %v", err)
		}
		if host.Running("host-all") {
			t.Fatal("expected host-all stopped")
		}
	})

	t.Run("timeout leaves tracked until done", func(t *testing.T) {
		unblock := make(chan struct{})
		host := newHostWithBlockingDiscovery(t, unblock)
		target := readyKubeTarget("host-all-timeout", nil)
		if err := host.StartIndexer(context.Background(), target); err != nil {
			t.Fatalf("StartIndexer: %v", err)
		}

		stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		err := host.StopAllIndexers(stopCtx)
		if err == nil {
			t.Fatal("expected StopAllIndexers timeout error")
		}
		if !host.Running("host-all-timeout") {
			t.Fatal("expected indexer left tracked after StopAllIndexers timeout")
		}

		close(unblock)
		deadline := time.After(2 * time.Second)
		for host.Running("host-all-timeout") {
			select {
			case <-deadline:
				t.Fatal("timed out waiting for background StopAllIndexers cleanup")
			case <-time.After(10 * time.Millisecond):
			}
		}
	})
}

func TestInProcessIndexController_TerminatingDuringFingerprintRestartSkipsStart(t *testing.T) {
	lister := &fakeTargetLister{}
	props := map[string]string{
		PropAPIServer:           "https://127.0.0.1:6443",
		PropServiceAccountToken: "token-1",
	}
	lister.set(readyKubeTarget("t1", props))
	runtime := newFakeIndexRuntime()

	enteredDesired := make(chan struct{})
	releaseDesired := make(chan struct{})
	var blockDesired atomic.Bool
	policy := desiredFuncPolicy(func(target domain.TargetInfo) (TargetIndexDecision, bool) {
		if blockDesired.Load() {
			select {
			case <-enteredDesired:
			default:
				close(enteredDesired)
			}
			<-releaseDesired
		}
		return DefaultInProcessIndexPolicy{}.Desired(target)
	})

	ctx, cancel := context.WithCancel(context.Background())
	ctrl := NewInProcessIndexController(
		lister,
		runtime,
		policy,
		slog.New(slog.DiscardHandler),
		WithReconcileInterval(time.Hour),
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctrl.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitFor(t, runtime.startedCh, "start")

	blockDesired.Store(true)
	props[PropServiceAccountToken] = "token-2"
	lister.set(readyKubeTarget("t1", props))
	ctrl.NotifyTargetReady(context.Background(), readyKubeTarget("t1", props))
	waitFor(t, enteredDesired, "desired blocked")

	ctrl.NotifyTargetTerminating(context.Background(), readyKubeTarget("t1", props))
	close(releaseDesired)
	waitFor(t, runtime.stoppedCh, "terminating or fingerprint stop")
	time.Sleep(30 * time.Millisecond)

	starts := 0
	for _, op := range runtime.callOps() {
		if op == "start:t1" {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("starts = %d, want 1 (restart skipped while terminating)", starts)
	}
}

type desiredFuncPolicy func(domain.TargetInfo) (TargetIndexDecision, bool)

func (f desiredFuncPolicy) Desired(target domain.TargetInfo) (TargetIndexDecision, bool) {
	return f(target)
}

func TestKubernetesInProcessIndexHost_DefaultFactoriesAndHostContextDeadline(t *testing.T) {
	hostCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	host := NewKubernetesInProcessIndexHost(hostCtx, nil, &recordingReporter{}, nil)
	target := readyKubeTarget("default-factories", map[string]string{
		PropAPIServer:           "https://127.0.0.1:1",
		PropServiceAccountToken: "token",
	})
	if err := host.StartIndexer(context.Background(), target); err != nil {
		t.Fatalf("StartIndexer with default factories: %v", err)
	}
	deadline := time.After(2 * time.Second)
	for host.Running("default-factories") {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for host context deadline to stop indexer")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestKubernetesInProcessIndexHost_ConcurrentStartIndexerIdempotent(t *testing.T) {
	var builds atomic.Int32
	releaseFirst := make(chan struct{})
	firstEntered := make(chan struct{})

	host := NewKubernetesInProcessIndexHost(
		context.Background(),
		nil,
		&recordingReporter{},
		slog.New(slog.DiscardHandler),
		WithInProcessIndexHostRESTConfigFactory(func(context.Context, domain.TargetInfo) (*rest.Config, error) {
			n := builds.Add(1)
			if n == 1 {
				close(firstEntered)
				<-releaseFirst
			}
			return &rest.Config{Host: "https://example"}, nil
		}),
		WithInProcessIndexHostDynamicClientFactory(func(*rest.Config) (dynamic.Interface, error) {
			return newFakeDynamicClient(podsGVR(), crdGVR), nil
		}),
		WithInProcessIndexHostDiscoveryClientFactory(func(*rest.Config) (discovery.DiscoveryInterface, error) {
			return newFakeDiscovery([]*metav1.APIResourceList{{
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{
					{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
				},
			}}), nil
		}),
		WithInProcessIndexHostIndexConfig(func(domain.TargetInfo) IndexConfig {
			return IndexConfig{BatchInterval: time.Hour}
		}),
	)

	target := readyKubeTarget("race-1", nil)
	errCh := make(chan error, 2)
	go func() { errCh <- host.StartIndexer(context.Background(), target) }()
	<-firstEntered
	go func() { errCh <- host.StartIndexer(context.Background(), target) }()
	// Let the second start finish and register, then release the first.
	time.Sleep(30 * time.Millisecond)
	close(releaseFirst)

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("StartIndexer: %v", err)
		}
	}
	if !host.Running("race-1") {
		t.Fatal("expected race-1 running")
	}
	if err := host.StopIndexer(context.Background(), target); err != nil {
		t.Fatalf("StopIndexer: %v", err)
	}
}
