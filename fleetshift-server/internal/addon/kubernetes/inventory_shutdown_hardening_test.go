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
	k8stesting "k8s.io/client-go/testing"
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
	var disc callCountingDiscovery
	disc.fakeDiscoveryWithPreferred = newFakeDiscovery([]*metav1.APIResourceList{{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{
			{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
		},
	}})
	mgr := NewInformerManager(nil, &disc, make(chan ResourceEvent, 1), make(chan ResyncEvent, 1), nil, nil, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mgr.discoverAndReconcile(ctx, nil, nil)
	if disc.calls.Load() != 0 {
		t.Fatal("ServerPreferredResources must not be called when ctx is already canceled")
	}
	if len(mgr.stoppers) != 0 {
		t.Fatalf("expected no stoppers, got %d", len(mgr.stoppers))
	}
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
			"clusters/target-1",
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
