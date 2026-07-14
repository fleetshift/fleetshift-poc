package kubernetes

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestBuildTargetRESTConfig_NoGlobalTimeout(t *testing.T) {
	cfg, err := BuildTargetRESTConfig(context.Background(), nil, readyKubeTarget("t1", map[string]string{
		PropAPIServer: "https://cluster.example:6443",
	}))
	if err != nil {
		t.Fatalf("BuildTargetRESTConfig: %v", err)
	}
	if cfg.Timeout != 0 {
		t.Fatalf("Timeout = %v, want 0 (watches share this config)", cfg.Timeout)
	}
}

func TestGenericInformer_SendEventUnblocksOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Unbuffered channel: send blocks until a receiver exists or ctx cancels.
		eventCh := make(chan ResourceEvent)
		resyncCh := make(chan ResyncEvent, 1)
		inf := NewInformer(nil, podsGVR(), eventCh, resyncCh, nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan bool, 1)
		go func() {
			done <- inf.sendEvent(ctx, ResourceEvent{Op: EventAdd, GVR: podsGVR()})
		}()

		synctest.Wait()
		select {
		case <-done:
			t.Fatal("sendEvent returned before cancel while channel was blocked")
		default:
		}

		cancel()
		synctest.Wait()
		ok := <-done
		if ok {
			t.Fatal("sendEvent should return false after cancel")
		}
	})
}

func TestGenericInformer_SendResyncUnblocksOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eventCh := make(chan ResourceEvent, 1)
		resyncCh := make(chan ResyncEvent) // unbuffered
		inf := NewInformer(nil, podsGVR(), eventCh, resyncCh, nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan bool, 1)
		go func() {
			done <- inf.sendResync(ctx, ResyncEvent{GVR: podsGVR()})
		}()

		synctest.Wait()
		cancel()
		synctest.Wait()
		ok := <-done
		if ok {
			t.Fatal("sendResync should return false after cancel")
		}
	})
}

func TestGenericInformer_ListAndResyncRespectsCancelDuringSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		pod := makePod("uid-1", "pod-1", "default", "1")
		if _, err := dyn.Resource(gvr).Namespace("default").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod: %v", err)
		}

		eventCh := make(chan ResourceEvent) // unbuffered: blocks on first send
		resyncCh := make(chan ResyncEvent, 1)
		inf := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- inf.listAndResync(ctx)
		}()

		synctest.Wait()
		cancel()
		synctest.Wait()

		err := <-errCh
		if err == nil {
			t.Fatal("expected listAndResync to return ctx error when send blocks")
		}
	})
}

func TestInformerManager_StopAllAwaitsInformerGoroutines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		disc := newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		}})
		dyn := newFakeDynamicClient(gvr)
		eventCh := make(chan ResourceEvent, 64)
		resyncCh := make(chan ResyncEvent, 8)
		stopAck := ackAllResyncs(resyncCh)
		defer stopAck()
		mgr := NewInformerManager(dyn, disc, eventCh, resyncCh, nil, nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		mgr.Reconcile(ctx, []schema.GroupVersionResource{gvr})
		synctest.Wait()
		if len(mgr.stoppers) != 1 {
			t.Fatalf("stoppers = %d, want 1", len(mgr.stoppers))
		}

		// Block the informer on a full event channel send after cancel by
		// filling the buffer, then prove StopAll still returns once cancel
		// unblocks the send (cancellation-aware send).
		cancel()
		if err := mgr.StopAll(context.Background()); err != nil {
			t.Fatalf("StopAll: %v", err)
		}
		if len(mgr.stoppers) != 0 {
			t.Fatalf("stoppers = %d after StopAll, want 0", len(mgr.stoppers))
		}
	})
}

func TestInformerManager_StopAllAwaitsCRDInformer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		disc := newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		}})
		dyn := newFakeDynamicClient(podsGVR(), crdGVR)
		eventCh := make(chan ResourceEvent, 64)
		resyncCh := make(chan ResyncEvent, 8)
		stopAck := ackAllResyncs(resyncCh)
		defer stopAck()
		mgr := NewInformerManager(dyn, disc, eventCh, resyncCh, nil, nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		runDone := make(chan struct{})
		go func() {
			defer close(runDone)
			mgr.runContinuous(ctx, nil, []Resource{{ApiGroups: []string{""}, Resources: []string{"pods"}}}, time.Hour)
		}()

		awaitRunContinuousReady()
		synctest.Wait()

		cancel()
		synctest.Wait()
		<-runDone

		if err := mgr.StopAll(context.Background()); err != nil {
			t.Fatalf("StopAll after RunContinuous: %v", err)
		}
	})
}

func TestInformerManager_StopAllTimeout(t *testing.T) {
	mgr := NewInformerManager(nil, newFakeDiscovery(nil), make(chan ResourceEvent, 1), make(chan ResyncEvent, 1), nil, nil, slog.Default())
	mgr.informerWG.Add(1) // never Done: simulates a stuck informer

	stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := mgr.StopAll(stopCtx)
	if err == nil {
		t.Fatal("expected StopAll timeout error")
	}

	mgr.informerWG.Done()
	if err := mgr.StopAll(context.Background()); err != nil {
		t.Fatalf("StopAll after release: %v", err)
	}
}

func TestDiscoverAndReconcile_SkipsWhenContextCanceled(t *testing.T) {
	var called atomic.Bool
	disc := &countingDiscovery{
		fakeDiscoveryWithPreferred: newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		}}),
		called: &called,
	}
	mgr := NewInformerManager(nil, disc, make(chan ResourceEvent, 1), make(chan ResyncEvent, 1), nil, nil, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mgr.discoverAndReconcile(ctx, nil, nil)
	if called.Load() {
		t.Fatal("ServerPreferredResources must not be called when ctx is already canceled")
	}
	if len(mgr.stoppers) != 0 {
		t.Fatalf("expected no stoppers, got %d", len(mgr.stoppers))
	}
}

type countingDiscovery struct {
	*fakeDiscoveryWithPreferred
	called *atomic.Bool
}

func (d *countingDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	d.called.Store(true)
	return d.fakeDiscoveryWithPreferred.ServerPreferredResources()
}

func TestWriter_StopUsesProvidedFlushContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var sawDeadline atomic.Bool
		var flushStarted sync.WaitGroup
		flushStarted.Add(1)
		releaseFlush := make(chan struct{})

		mock := &recordingReporter{
			applyDeltaFunc: func(ctx context.Context, _ InventoryDeltaReport) error {
				if _, ok := ctx.Deadline(); ok {
					sawDeadline.Store(true)
				}
				flushStarted.Done()
				<-releaseFlush
				return ctx.Err()
			},
		}
		w := newTestWriter(mock, nil, nil, time.Hour)

		runCtx, runCancel := context.WithCancel(context.Background())
		defer runCancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(runCtx)
		}()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
		synctest.Wait()

		flushCtx, flushCancel := context.WithTimeout(context.Background(), time.Second)
		defer flushCancel()
		w.Stop(flushCtx)

		synctest.Wait()
		flushStarted.Wait()
		if !sawDeadline.Load() {
			t.Fatal("final flush must use Stop's deadline-bearing context")
		}
		close(releaseFlush)
		synctest.Wait()
		<-done
	})
}

func TestWriter_ShutdownFlushInheritsContextDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var gotTimeout time.Duration
		var mu sync.Mutex
		mock := &recordingReporter{
			applyDeltaFunc: func(ctx context.Context, _ InventoryDeltaReport) error {
				if dl, ok := ctx.Deadline(); ok {
					mu.Lock()
					gotTimeout = time.Until(dl)
					mu.Unlock()
				}
				return nil
			},
		}
		w := newTestWriter(mock, nil, nil, time.Hour)

		runCtx, runCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(runCtx)
		}()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
		synctest.Wait()
		runCancel()
		synctest.Wait()
		<-done

		mu.Lock()
		defer mu.Unlock()
		if gotTimeout <= 0 || gotTimeout > 250*time.Millisecond {
			t.Fatalf("flush remaining deadline = %v, want (0, 250ms]", gotTimeout)
		}
	})
}

func TestIndexerDelegate_DoneClosesOnlyAfterWriterFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		disc := newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		}})
		dyn := newFakeDynamicClient(gvr, crdGVR)
		pod := makePod("uid-pending", "pending", "default", "1")
		if _, err := dyn.Resource(gvr).Namespace("default").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod: %v", err)
		}

		var shutdownFlushOnce sync.Once
		flushEntered := make(chan struct{})
		releaseFlush := make(chan struct{})
		var callsAfterDone atomic.Int32
		doneClosed := make(chan struct{})

		mock := &recordingReporter{
			applyDeltaFunc: func(ctx context.Context, _ InventoryDeltaReport) error {
				select {
				case <-doneClosed:
					callsAfterDone.Add(1)
				default:
				}
				// Indexer Stop passes a deadline-bearing shutdown context.
				if _, ok := ctx.Deadline(); ok {
					shutdownFlushOnce.Do(func() { close(flushEntered) })
					<-releaseFlush
				}
				return nil
			},
		}

		ic := newIndexerDelegate(
			"target-1",
			dyn,
			disc,
			mock,
			NoopEdgeSink{},
			IndexConfig{
				Schema: IndexSchema{Entries: map[schema.GroupVersionResource]SchemaEntry{
					gvr: {GVR: gvr, Kind: "Pod"},
				}},
				AllowList:     []Resource{{ApiGroups: []string{""}, Resources: []string{"pods"}}},
				BatchInterval: time.Hour, // keep EventAdd pending until shutdown flush
			},
			slog.Default(),
		)

		ctx, cancel := context.WithCancel(context.Background())
		go ic.start(ctx)

		awaitRunContinuousReady()
		synctest.Wait()
		// Allow LIST EventAdd to land in the writer pending batch.
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()

		cancel()
		synctest.Wait()

		select {
		case <-flushEntered:
		default:
			t.Fatal("expected shutdown flush to start while holding done open")
		}

		select {
		case <-ic.done:
			t.Fatal("indexer done closed before writer flush was released")
		default:
		}

		close(releaseFlush)
		synctest.Wait()
		<-ic.done
		close(doneClosed)

		if callsAfterDone.Load() != 0 {
			t.Fatalf("reporter called %d times after done closed", callsAfterDone.Load())
		}
	})
}

func TestKubernetesInProcessIndexHost_StopAllCancelsAllBeforeWait(t *testing.T) {
	unblockSlow := make(chan struct{})
	var slowStarted atomic.Bool
	var discoveryClients atomic.Int32

	host := NewKubernetesInProcessIndexHost(
		context.Background(),
		nil,
		&recordingReporter{},
		slog.New(slog.DiscardHandler),
		WithInProcessIndexHostRESTConfigFactory(func(context.Context, domain.TargetInfo) (*rest.Config, error) {
			return &rest.Config{Host: "https://example", BearerToken: "test-token"}, nil
		}),
		WithInProcessIndexHostDynamicClientFactory(func(*rest.Config) (dynamic.Interface, error) {
			return newFakeDynamicClient(podsGVR(), crdGVR), nil
		}),
		WithInProcessIndexHostDiscoveryClientFactory(func(*rest.Config) (discovery.DiscoveryInterface, error) {
			n := discoveryClients.Add(1)
			if n == 1 {
				// First target (slow): readiness succeeds, then RunContinuous blocks.
				return &readyThenBlockDiscovery{
					fakeDiscoveryWithPreferred: newFakeDiscovery([]*metav1.APIResourceList{{
						GroupVersion: "v1",
						APIResources: []metav1.APIResource{
							{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
						},
					}}),
					unblock: unblockSlow,
					onBlock: func() { slowStarted.Store(true) },
				}, nil
			}
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

	slow := readyKubeTarget("slow", nil)
	fast := readyKubeTarget("fast", nil)
	if err := host.StartIndexer(context.Background(), slow); err != nil {
		t.Fatalf("StartIndexer slow: %v", err)
	}
	deadline := time.After(2 * time.Second)
	for !slowStarted.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for slow discovery to block")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := host.StartIndexer(context.Background(), fast); err != nil {
		t.Fatalf("StartIndexer fast: %v", err)
	}

	// Give the fast indexer a moment to pass discovery and become stoppable.
	time.Sleep(50 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := host.StopAllIndexers(stopCtx)
	if err == nil {
		t.Fatal("expected timeout error from slow indexer")
	}

	// Fast indexer must finish within the shared deadline because it was
	// canceled immediately (not after the slow wait consumed the budget).
	if host.HasIndexer("fast") {
		t.Fatal("fast indexer should have stopped under the shared deadline")
	}
	if !host.HasIndexer("slow") {
		t.Fatal("slow indexer should remain tracked after timeout")
	}

	close(unblockSlow)
	deadline = time.After(2 * time.Second)
	for host.HasIndexer("slow") {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for slow indexer cleanup")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestWatch_SendUnblocksOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		fakeWatch := watch.NewFake()
		dyn.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
			return true, fakeWatch, nil
		})

		// Unbuffered: watch blocks on the first event send.
		eventCh := make(chan ResourceEvent)
		resyncCh := make(chan ResyncEvent, 1)
		stopAck := ackAllResyncs(resyncCh)
		defer stopAck()
		inf := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())
		inf.watchResourceVersion = "1"

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			inf.watch(ctx)
		}()
		synctest.Wait()

		fakeWatch.Add(makePod("uid-1", "pod-1", "default", "2"))
		synctest.Wait()

		select {
		case <-done:
			t.Fatal("watch returned before cancel while send was blocked")
		default:
		}

		cancel()
		synctest.Wait()
		<-done
	})
}

func TestInformerManager_StopAllUnblocksFullEventBuffer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		pod := makePod("uid-1", "pod-1", "default", "1")
		if _, err := dyn.Resource(gvr).Namespace("default").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod: %v", err)
		}

		// Capacity 0: LIST's first EventAdd blocks inside the informer.
		eventCh := make(chan ResourceEvent)
		resyncCh := make(chan ResyncEvent, 1)
		stopAck := ackAllResyncs(resyncCh)
		defer stopAck()
		mgr := NewInformerManager(dyn, newFakeDiscovery(nil), eventCh, resyncCh, nil, nil, slog.Default())

		informer := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())
		informerCtx, cancel := context.WithCancel(context.Background())
		mgr.stoppers[gvr] = cancel
		mgr.startInformer(informer, informerCtx)

		synctest.Wait()

		stopErr := make(chan error, 1)
		go func() { stopErr <- mgr.StopAll(context.Background()) }()
		synctest.Wait()

		if err := <-stopErr; err != nil {
			t.Fatalf("StopAll with blocked informer send: %v", err)
		}
		if len(mgr.stoppers) != 0 {
			t.Fatalf("stoppers = %d, want 0", len(mgr.stoppers))
		}
	})
}

func TestWriter_StopDeliversFlushContextWhileRunActive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var usedStopCtx atomic.Bool
		flushStarted := make(chan struct{})
		mock := &recordingReporter{
			applyDeltaFunc: func(ctx context.Context, _ InventoryDeltaReport) error {
				if v := ctx.Value(shutdownFlushMarker{}); v != nil {
					usedStopCtx.Store(true)
				}
				select {
				case <-flushStarted:
				default:
					close(flushStarted)
				}
				return nil
			},
		}
		w := newTestWriter(mock, nil, nil, time.Hour)

		runCtx, runCancel := context.WithCancel(context.Background())
		defer runCancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(runCtx)
		}()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
		synctest.Wait()

		flushCtx := context.WithValue(context.Background(), shutdownFlushMarker{}, true)
		flushCtx, flushCancel := context.WithTimeout(flushCtx, time.Second)
		defer flushCancel()
		w.Stop(flushCtx)

		synctest.Wait()
		select {
		case <-flushStarted:
		default:
			t.Fatal("expected Stop to trigger a flush while Run is active")
		}
		<-done

		if !usedStopCtx.Load() {
			t.Fatal("final flush must use Stop's context")
		}
	})
}

func TestWriter_DoubleStopAndStopAfterExitAreSafe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, time.Hour)

		runCtx, runCancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(runCtx)
		}()
		synctest.Wait()

		flushCtx := context.Background()
		w.Stop(flushCtx)
		w.Stop(flushCtx) // second Stop must not block
		synctest.Wait()
		<-done

		w.Stop(flushCtx) // after exit
		runCancel()
	})
}

func TestShutdownSequence_InformerTimeoutStillStopsWriter(t *testing.T) {
	// Mirrors indexerDelegate shutdown: StopAll may time out, but the writer
	// must still be stopped under the shared budget and return.
	mock := &recordingReporter{}
	w := newTestWriter(mock, nil, nil, time.Hour)
	writerCtx, writerCancel := context.WithCancel(context.Background())
	defer writerCancel()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		w.Run(writerCtx)
	}()

	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
	time.Sleep(20 * time.Millisecond)

	mgr := NewInformerManager(nil, newFakeDiscovery(nil), make(chan ResourceEvent, 1), make(chan ResyncEvent, 1), nil, nil, slog.Default())
	mgr.informerWG.Add(1) // stuck informer

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shutdownCancel()

	if err := mgr.StopAll(shutdownCtx); err == nil {
		t.Fatal("expected StopAll timeout")
	}

	w.Stop(shutdownCtx)
	select {
	case <-writerDone:
	case <-time.After(2 * time.Second):
		writerCancel()
		t.Fatal("writer did not stop after informer StopAll timeout")
	}
	mgr.informerWG.Done()
}

type shutdownFlushMarker struct{}

func TestKubernetesInProcessIndexHost_StartDuringStopAllTimeout(t *testing.T) {
	unblock := make(chan struct{})
	host, blocked := newHostWithReadyThenBlockDiscovery(t, unblock)
	target := readyKubeTarget("overlap", nil)

	if err := host.StartIndexer(context.Background(), target); err != nil {
		t.Fatalf("StartIndexer: %v", err)
	}
	awaitDiscoveryBlocked(t, blocked)

	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := host.StopAllIndexers(stopCtx); err == nil {
		t.Fatal("expected StopAllIndexers timeout")
	}
	if !host.HasIndexer("overlap") {
		t.Fatal("expected indexer still tracked after timeout")
	}

	// While the timed-out indexer remains tracked, StartIndexer must be a
	// no-op (no duplicate goroutine) rather than racing a second start.
	if err := host.StartIndexer(context.Background(), target); err != nil {
		t.Fatalf("StartIndexer during timeout cleanup: %v", err)
	}
	if !host.HasIndexer("overlap") {
		t.Fatal("expected same indexer still tracked")
	}

	close(unblock)
	deadline := time.After(2 * time.Second)
	for host.HasIndexer("overlap") {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for background cleanup")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// After cleanup, a fresh start must succeed.
	if err := host.StartIndexer(context.Background(), target); err != nil {
		t.Fatalf("StartIndexer after cleanup: %v", err)
	}
	if !host.HasIndexer("overlap") {
		t.Fatal("expected indexer running after restart")
	}
	if err := host.StopIndexer(context.Background(), target.ID()); err != nil {
		t.Fatalf("StopIndexer: %v", err)
	}
}

func TestKubernetesInProcessIndexHost_ConcurrentStopIndexerAndStopAll(t *testing.T) {
	host := NewKubernetesInProcessIndexHost(
		context.Background(),
		nil,
		&recordingReporter{},
		slog.New(slog.DiscardHandler),
		WithInProcessIndexHostRESTConfigFactory(func(context.Context, domain.TargetInfo) (*rest.Config, error) {
			return &rest.Config{Host: "https://example", BearerToken: "test-token"}, nil
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

	t1 := readyKubeTarget("a", nil)
	t2 := readyKubeTarget("b", nil)
	for _, target := range []domain.TargetInfo{t1, t2} {
		if err := host.StartIndexer(context.Background(), target); err != nil {
			t.Fatalf("StartIndexer %s: %v", target.ID(), err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	errCh := make(chan error, 3)
	wg.Add(3)
	go func() {
		defer wg.Done()
		errCh <- host.StopIndexer(context.Background(), t1.ID())
	}()
	go func() {
		defer wg.Done()
		errCh <- host.StopIndexer(context.Background(), t2.ID())
	}()
	go func() {
		defer wg.Done()
		errCh <- host.StopAllIndexers(context.Background())
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent stop: %v", err)
		}
	}
	if host.HasIndexer("a") || host.HasIndexer("b") {
		t.Fatal("expected all indexers stopped")
	}
}

type lateCallReporter struct {
	recordingReporter
	afterDone *atomic.Bool
	late      *atomic.Int32
}

func (r *lateCallReporter) note() {
	if r.afterDone.Load() {
		r.late.Add(1)
	}
}

func (r *lateCallReporter) ApplyDelta(ctx context.Context, delta InventoryDeltaReport) error {
	r.note()
	return r.recordingReporter.ApplyDelta(ctx, delta)
}

func TestControllerShutdown_NoReporterCallsAfterRunReturns(t *testing.T) {
	var afterDone atomic.Bool
	var late atomic.Int32
	reporter := &lateCallReporter{afterDone: &afterDone, late: &late}

	gvr := podsGVR()
	dyn := newFakeDynamicClient(gvr, crdGVR)
	pod := makePod("uid-1", "pod-1", "default", "1")
	if _, err := dyn.Resource(gvr).Namespace("default").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	disc := newFakeDiscovery([]*metav1.APIResourceList{{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{
			{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
		},
	}})

	hostCtx, hostCancel := context.WithCancel(context.Background())
	defer hostCancel()

	host := NewKubernetesInProcessIndexHost(
		hostCtx,
		nil,
		reporter,
		slog.New(slog.DiscardHandler),
		WithInProcessIndexHostRESTConfigFactory(func(context.Context, domain.TargetInfo) (*rest.Config, error) {
			return &rest.Config{Host: "https://example", BearerToken: "test-token"}, nil
		}),
		WithInProcessIndexHostDynamicClientFactory(func(*rest.Config) (dynamic.Interface, error) {
			return dyn, nil
		}),
		WithInProcessIndexHostDiscoveryClientFactory(func(*rest.Config) (discovery.DiscoveryInterface, error) {
			return disc, nil
		}),
		WithInProcessIndexHostIndexConfig(func(domain.TargetInfo) IndexConfig {
			return IndexConfig{
				Schema: IndexSchema{Entries: map[schema.GroupVersionResource]SchemaEntry{
					gvr: {GVR: gvr, Kind: "Pod"},
				}},
				AllowList:     []Resource{{ApiGroups: []string{""}, Resources: []string{"pods"}}},
				BatchInterval: 50 * time.Millisecond,
			}
		}),
	)

	target := readyKubeTarget("stack", nil)
	lister := &fakeTargetLister{}
	lister.set(target)

	ctrlCtx, ctrlCancel := context.WithCancel(hostCtx)
	ctrl := NewInProcessIndexController(
		lister,
		host,
		DefaultInProcessIndexPolicy{},
		slog.New(slog.DiscardHandler),
		WithReconcileInterval(time.Hour),
		WithStopTimeout(2*time.Second),
	)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		ctrl.Run(ctrlCtx)
	}()

	deadline := time.After(2 * time.Second)
	for !host.HasIndexer("stack") {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for indexer start")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Allow at least one inventory report before shutdown.
	time.Sleep(200 * time.Millisecond)

	ctrlCancel()
	hostCancel()
	<-runDone
	afterDone.Store(true)

	// Catch stragglers that race past Run's return.
	time.Sleep(200 * time.Millisecond)
	if late.Load() != 0 {
		t.Fatalf("reporter called %d times after controller.Run returned", late.Load())
	}
	if host.HasIndexer("stack") {
		t.Fatal("expected indexer stopped when Run returned")
	}
}
