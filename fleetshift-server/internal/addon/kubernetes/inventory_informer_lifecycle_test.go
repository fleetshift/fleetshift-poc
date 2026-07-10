package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	k8stesting "k8s.io/client-go/testing"
)

func podsGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
}

func makePod(uid, name, namespace, rv string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"uid":             uid,
			"name":            name,
			"namespace":       namespace,
			"resourceVersion": rv,
		},
	}}
	return obj
}

func TestDrainChannel(t *testing.T) {
	ch := make(chan ResourceEvent, 4)
	ch <- ResourceEvent{Op: EventAdd}
	ch <- ResourceEvent{Op: EventUpdate}
	ch <- ResourceEvent{Op: EventDelete}
	drainChannel(ch)
	select {
	case <-ch:
		t.Fatal("expected channel to be drained")
	default:
	}
}

func TestDrainChannel_Empty(t *testing.T) {
	ch := make(chan ResourceEvent, 1)
	drainChannel(ch) // must not block
}

func TestWaitUntilInitialized_AlreadyInitialized(t *testing.T) {
	i := &GenericInformer{logger: slog.Default()}
	i.initialized.Store(true)
	i.WaitUntilInitialized(context.Background(), time.Hour)
}

func TestWaitUntilInitialized_Timeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		i := &GenericInformer{logger: slog.Default()}
		done := make(chan struct{})
		go func() {
			defer close(done)
			i.WaitUntilInitialized(context.Background(), time.Second)
		}()
		synctest.Wait()
		time.Sleep(time.Second + time.Millisecond)
		synctest.Wait()
		<-done
		if i.initialized.Load() {
			t.Fatal("timeout path must not mark informer initialized")
		}
	})
}

func TestWaitUntilInitialized_ContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		i := &GenericInformer{logger: slog.Default()}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			i.WaitUntilInitialized(ctx, time.Hour)
		}()
		synctest.Wait()
		cancel()
		synctest.Wait()
		<-done
	})
}

func TestListAndResync_EmitsAddsAndResync(t *testing.T) {
	gvr := podsGVR()
	dyn := newFakeDynamicClient(gvr)
	pod := makePod("uid-1", "pod-1", "default", "10")
	if _, err := dyn.Resource(gvr).Namespace("default").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create: %v", err)
	}

	eventCh := make(chan ResourceEvent, 10)
	resyncCh := make(chan ResyncEvent, 10)
	inf := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())

	if err := inf.listAndResync(context.Background()); err != nil {
		t.Fatalf("listAndResync: %v", err)
	}

	select {
	case ev := <-eventCh:
		if ev.Op != EventAdd || ev.Resource.GetUID() != "uid-1" {
			t.Fatalf("unexpected event: %+v", ev)
		}
	default:
		t.Fatal("expected EventAdd")
	}
	select {
	case rs := <-resyncCh:
		if rs.GVR != gvr || len(rs.Resources) != 1 {
			t.Fatalf("unexpected resync: %+v", rs)
		}
	default:
		t.Fatal("expected ResyncEvent")
	}
	if inf.resourceIndex["uid-1"] != "10" {
		t.Fatalf("resourceIndex = %#v", inf.resourceIndex)
	}
}

func TestListAndResync_SkipsDisallowedNamespaces(t *testing.T) {
	gvr := podsGVR()
	dyn := newFakeDynamicClient(gvr)
	allowed := makePod("uid-ok", "ok", "prod-us", "1")
	denied := makePod("uid-no", "no", "default", "1")
	for _, obj := range []*unstructured.Unstructured{allowed, denied} {
		if _, err := dyn.Resource(gvr).Namespace(obj.GetNamespace()).Create(context.Background(), obj, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create: %v", err)
		}
	}

	eventCh := make(chan ResourceEvent, 10)
	resyncCh := make(chan ResyncEvent, 10)
	filter := NewNamespaceFilter(NamespaceFilterConfig{IncludePatterns: []string{"prod-*"}})
	inf := NewInformer(dyn, gvr, eventCh, resyncCh, filter, slog.Default())

	if err := inf.listAndResync(context.Background()); err != nil {
		t.Fatalf("listAndResync: %v", err)
	}

	if len(inf.resourceIndex) != 1 || inf.resourceIndex["uid-ok"] == "" {
		t.Fatalf("resourceIndex = %#v, want only uid-ok", inf.resourceIndex)
	}
	rs := <-resyncCh
	if len(rs.Resources) != 1 || rs.Resources[0].GetUID() != "uid-ok" {
		t.Fatalf("resync resources = %v", rs.Resources)
	}
}

func TestListAndResync_PrunesStaleIndexWithoutEventDelete(t *testing.T) {
	gvr := podsGVR()
	dyn := newFakeDynamicClient(gvr)
	eventCh := make(chan ResourceEvent, 10)
	resyncCh := make(chan ResyncEvent, 10)
	inf := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())
	inf.resourceIndex = map[string]string{"stale-uid": "1"}

	if err := inf.listAndResync(context.Background()); err != nil {
		t.Fatalf("listAndResync: %v", err)
	}

	// Stale absence is owned by ResyncEvent → ReplaceCollection, not
	// per-UID EventDelete (which would duplicate the collection prune).
	select {
	case ev := <-eventCh:
		t.Fatalf("unexpected event on empty LIST: op=%v uid=%s", ev.Op, ev.Resource.GetUID())
	default:
	}
	if _, ok := inf.resourceIndex["stale-uid"]; ok {
		t.Fatal("stale uid should be removed from resourceIndex")
	}
	select {
	case rs := <-resyncCh:
		if rs.GVR != gvr {
			t.Fatalf("resync GVR = %v, want %v", rs.GVR, gvr)
		}
		if len(rs.Resources) != 0 {
			t.Fatalf("resync resources = %d, want 0", len(rs.Resources))
		}
	default:
		t.Fatal("expected ResyncEvent")
	}
}

func TestListAndResync_ListError(t *testing.T) {
	gvr := podsGVR()
	dyn := newFakeDynamicClient(gvr)
	dyn.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("list failed")
	})

	eventCh := make(chan ResourceEvent, 10)
	resyncCh := make(chan ResyncEvent, 10)
	inf := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())

	err := inf.listAndResync(context.Background())
	if err == nil {
		t.Fatal("expected list error")
	}
	if inf.retries != 1 {
		t.Fatalf("retries = %d, want 1", inf.retries)
	}
}

func TestListAndResync_Pagination(t *testing.T) {
	gvr := podsGVR()
	dyn := newFakeDynamicClient(gvr)
	page := 0
	dyn.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		page++
		pod := makePod(fmt.Sprintf("uid-%d", page), fmt.Sprintf("pod-%d", page), "default", fmt.Sprintf("%d", page))
		list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{*pod}}
		if page == 1 {
			list.Object = map[string]any{
				"metadata": map[string]any{
					"resourceVersion":    "1",
					"continue":           "token-2",
					"remainingItemCount": int64(1),
				},
			}
		} else {
			list.Object = map[string]any{
				"metadata": map[string]any{
					"resourceVersion": "2",
				},
			}
		}
		return true, list, nil
	})

	eventCh := make(chan ResourceEvent, 10)
	resyncCh := make(chan ResyncEvent, 10)
	inf := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())

	if err := inf.listAndResync(context.Background()); err != nil {
		t.Fatalf("listAndResync: %v", err)
	}
	if page != 2 {
		t.Fatalf("expected 2 list pages, got %d", page)
	}
	if len(inf.resourceIndex) != 2 {
		t.Fatalf("resourceIndex = %#v", inf.resourceIndex)
	}
	rs := <-resyncCh
	if len(rs.Resources) != 2 {
		t.Fatalf("resync len = %d, want 2", len(rs.Resources))
	}
	if inf.listRV != "2" {
		t.Fatalf("listRV = %q, want 2", inf.listRV)
	}
}

func TestWatch_AddUpdateDelete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		fakeWatch := watch.NewFake()
		dyn.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
			return true, fakeWatch, nil
		})

		eventCh := make(chan ResourceEvent, 10)
		resyncCh := make(chan ResyncEvent, 10)
		inf := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())
		inf.listRV = "1"

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			inf.watch(ctx)
		}()
		synctest.Wait()

		pod := makePod("uid-1", "pod-1", "default", "2")
		fakeWatch.Add(pod)
		synctest.Wait()
		ev := <-eventCh
		if ev.Op != EventAdd || string(ev.Resource.GetUID()) != "uid-1" {
			t.Fatalf("add event: %+v", ev)
		}

		pod.SetResourceVersion("3")
		fakeWatch.Modify(pod)
		synctest.Wait()
		ev = <-eventCh
		if ev.Op != EventUpdate {
			t.Fatalf("update event op = %v", ev.Op)
		}
		if inf.resourceIndex["uid-1"] != "3" {
			t.Fatalf("resourceIndex after update = %#v", inf.resourceIndex)
		}

		fakeWatch.Delete(pod)
		synctest.Wait()
		ev = <-eventCh
		if ev.Op != EventDelete {
			t.Fatalf("delete event op = %v", ev.Op)
		}
		if _, ok := inf.resourceIndex["uid-1"]; ok {
			t.Fatal("uid should be removed after delete")
		}

		cancel()
		synctest.Wait()
		<-done
	})
}

func TestWatch_NamespaceFilter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		fakeWatch := watch.NewFake()
		dyn.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
			return true, fakeWatch, nil
		})

		eventCh := make(chan ResourceEvent, 10)
		resyncCh := make(chan ResyncEvent, 10)
		filter := NewNamespaceFilter(NamespaceFilterConfig{IncludePatterns: []string{"prod-*"}})
		inf := NewInformer(dyn, gvr, eventCh, resyncCh, filter, slog.Default())
		inf.listRV = "1"

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			inf.watch(ctx)
		}()
		synctest.Wait()

		fakeWatch.Add(makePod("uid-denied", "p", "default", "1"))
		fakeWatch.Add(makePod("uid-ok", "p", "prod-us", "1"))
		synctest.Wait()

		ev := <-eventCh
		if string(ev.Resource.GetUID()) != "uid-ok" {
			t.Fatalf("expected only allowed namespace event, got %s", ev.Resource.GetUID())
		}
		select {
		case ev := <-eventCh:
			t.Fatalf("unexpected extra event: %+v", ev)
		default:
		}

		cancel()
		synctest.Wait()
		<-done
	})
}

func TestWatch_ErrorEndsWatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		fakeWatch := watch.NewFake()
		dyn.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
			return true, fakeWatch, nil
		})

		eventCh := make(chan ResourceEvent, 10)
		resyncCh := make(chan ResyncEvent, 10)
		inf := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())
		inf.listRV = "1"

		done := make(chan struct{})
		go func() {
			defer close(done)
			inf.watch(context.Background())
		}()
		synctest.Wait()

		fakeWatch.Error(&metav1.Status{Status: metav1.StatusFailure, Message: "boom"})
		synctest.Wait()
		<-done
	})
}

func TestWatch_UnexpectedTypeEndsWatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		fakeWatch := watch.NewFake()
		dyn.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
			return true, fakeWatch, nil
		})

		eventCh := make(chan ResourceEvent, 10)
		resyncCh := make(chan ResyncEvent, 10)
		inf := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())
		inf.listRV = "1"

		done := make(chan struct{})
		go func() {
			defer close(done)
			inf.watch(context.Background())
		}()
		synctest.Wait()

		fakeWatch.Action(watch.Bookmark, makePod("uid-1", "pod-1", "default", "1"))
		synctest.Wait()
		<-done
	})
}

func TestWatch_StartErrorIncrementsRetries(t *testing.T) {
	gvr := podsGVR()
	dyn := newFakeDynamicClient(gvr)
	dyn.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
		return true, nil, errors.New("watch failed")
	})

	eventCh := make(chan ResourceEvent, 10)
	resyncCh := make(chan ResyncEvent, 10)
	inf := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())
	inf.listRV = "1"
	inf.watch(context.Background())
	if inf.retries != 1 {
		t.Fatalf("retries = %d, want 1", inf.retries)
	}
}

func TestWatch_ChannelClosedEndsWatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		fakeWatch := watch.NewFake()
		dyn.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
			return true, fakeWatch, nil
		})

		eventCh := make(chan ResourceEvent, 10)
		resyncCh := make(chan ResyncEvent, 10)
		inf := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())
		inf.listRV = "1"

		done := make(chan struct{})
		go func() {
			defer close(done)
			inf.watch(context.Background())
		}()
		synctest.Wait()
		fakeWatch.Stop()
		synctest.Wait()
		<-done
	})
}

func TestGenericInformer_Run_ShutdownDoesNotEmitDeletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		pod := makePod("uid-1", "pod-1", "default", "1")
		if _, err := dyn.Resource(gvr).Namespace("default").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create: %v", err)
		}

		eventCh := make(chan ResourceEvent, 20)
		resyncCh := make(chan ResyncEvent, 10)
		inf := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			inf.Run(ctx)
		}()
		synctest.Wait()

		// Drain list/watch startup events.
		deadline := time.After(time.Second)
	drain:
		for {
			select {
			case <-eventCh:
			case <-resyncCh:
			case <-deadline:
				break drain
			default:
				if inf.initialized.Load() {
					break drain
				}
				synctest.Wait()
			}
		}

		cancel()
		synctest.Wait()
		<-done

		for {
			select {
			case ev := <-eventCh:
				if ev.Op == EventDelete {
					t.Fatalf("informer shutdown must not emit EventDelete, got uid=%s", ev.Resource.GetUID())
				}
			default:
				return
			}
		}
	})
}

func TestDiscoverAndReconcile_StartsAllowedInformers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		disc := newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
				{Name: "secrets", Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		}})
		dyn := newFakeDynamicClient(gvr, schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"})
		eventCh := make(chan ResourceEvent, 10)
		resyncCh := make(chan ResyncEvent, 10)
		mgr := NewInformerManager(dyn, disc, eventCh, resyncCh, nil, nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		mgr.discoverAndReconcile(ctx, nil, []Resource{{ApiGroups: []string{""}, Resources: []string{"pods"}}})
		synctest.Wait()

		if len(mgr.stoppers) != 1 {
			t.Fatalf("stoppers = %d, want 1", len(mgr.stoppers))
		}
		if _, ok := mgr.stoppers[gvr]; !ok {
			t.Fatalf("expected pods stopper, got %#v", mgr.stoppers)
		}
		mgr.StopAll()
		synctest.Wait()
	})
}

func TestDiscoverAndReconcile_DiscoveryErrorNilSupported(t *testing.T) {
	disc := &fakeDiscoveryPartial{
		fakeDiscoveryWithPreferred: newFakeDiscovery(nil),
		err:                        errors.New("discovery failed"),
		nilResources:               true,
	}
	mgr := NewInformerManager(nil, disc, make(chan ResourceEvent, 1), make(chan ResyncEvent, 1), nil, nil, slog.Default())
	mgr.discoverAndReconcile(context.Background(), nil, nil)
	if len(mgr.stoppers) != 0 {
		t.Fatalf("expected no stoppers on hard discovery failure, got %d", len(mgr.stoppers))
	}
}

// awaitRunContinuousReady advances fake time so RunContinuous can finish CRD
// informer startup and the initial timer drain before the test interacts with it.
// synctest.Wait alone returns at the first durable block, which is often still
// inside WaitUntilInitialized.
func awaitRunContinuousReady() {
	time.Sleep(time.Second)
}

func TestRunContinuous_InitialReconcileThenCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		disc := newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		}})
		dyn := newFakeDynamicClient(gvr, crdGVR)
		eventCh := make(chan ResourceEvent, 100)
		resyncCh := make(chan ResyncEvent, 100)
		mgr := NewInformerManager(dyn, disc, eventCh, resyncCh, nil, nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			mgr.RunContinuous(ctx, nil, nil)
		}()
		awaitRunContinuousReady()
		synctest.Wait()

		if _, ok := mgr.stoppers[gvr]; !ok {
			t.Fatalf("expected pods informer after initial reconcile, stoppers=%#v", mgr.stoppers)
		}

		cancel()
		synctest.Wait()
		<-done
	})
}

func TestRunContinuous_CRDEventTriggersReconcile(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		disc := newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		}})
		dyn := newFakeDynamicClient(gvr, crdGVR)
		eventCh := make(chan ResourceEvent, 100)
		resyncCh := make(chan ResyncEvent, 100)
		mgr := NewInformerManager(dyn, disc, eventCh, resyncCh, nil, nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			mgr.RunContinuous(ctx, nil, nil)
		}()
		awaitRunContinuousReady()
		synctest.Wait()

		// Let the startup CRD-resync throttle timer fire so pending clears.
		// The next CRD watch event then takes the event-path schedule branch.
		time.Sleep(10 * time.Second)
		synctest.Wait()

		before := len(mgr.stoppers)

		crd := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apiextensions.k8s.io/v1",
			"kind":       "CustomResourceDefinition",
			"metadata": map[string]any{
				"name":            "widgets.example.com",
				"uid":             "crd-uid-1",
				"resourceVersion": "1",
			},
		}}
		if _, err := dyn.Resource(crdGVR).Create(context.Background(), crd, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create crd: %v", err)
		}

		synctest.Wait()
		time.Sleep(10 * time.Second)
		synctest.Wait()

		if len(mgr.stoppers) < before {
			t.Fatalf("stoppers shrank after CRD reconcile: before=%d after=%d", before, len(mgr.stoppers))
		}

		cancel()
		synctest.Wait()
		<-done
	})
}

func TestSupportedResources_PartialErrorStillReturnsWatchable(t *testing.T) {
	disc := &fakeDiscoveryPartial{
		fakeDiscoveryWithPreferred: newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Verbs: metav1.Verbs{"list", "watch"}},
			},
		}}),
		err: errors.New("partial discovery failure"),
	}

	result, err := SupportedResources(disc, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := result[podsGVR()]; !ok {
		t.Fatalf("expected pods in result, got %#v", result)
	}
}

func TestSupportedResources_HardError(t *testing.T) {
	disc := &fakeDiscoveryPartial{
		fakeDiscoveryWithPreferred: newFakeDiscovery(nil),
		err:                        errors.New("discovery unavailable"),
		nilResources:               true,
	}
	_, err := SupportedResources(disc, slog.Default())
	if err == nil {
		t.Fatal("expected hard discovery error")
	}
}

func TestWatch_NonUnstructuredObjectsIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		fakeWatch := watch.NewFake()
		dyn.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
			return true, fakeWatch, nil
		})

		eventCh := make(chan ResourceEvent, 10)
		inf := NewInformer(dyn, gvr, eventCh, make(chan ResyncEvent, 10), nil, slog.Default())
		inf.listRV = "1"

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			inf.watch(ctx)
		}()
		synctest.Wait()

		status := &metav1.Status{Status: metav1.StatusFailure, Message: "not unstructured"}
		fakeWatch.Action(watch.Added, status)
		fakeWatch.Action(watch.Modified, status)
		fakeWatch.Action(watch.Deleted, status)
		fakeWatch.Add(makePod("uid-ok", "pod-ok", "default", "1"))
		synctest.Wait()

		ev := <-eventCh
		if string(ev.Resource.GetUID()) != "uid-ok" {
			t.Fatalf("expected only unstructured add, got %+v", ev)
		}
		select {
		case ev := <-eventCh:
			t.Fatalf("unexpected extra event: %+v", ev)
		default:
		}

		cancel()
		synctest.Wait()
		<-done
	})
}

func TestWatch_NamespaceFilterOnModifyAndDelete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		fakeWatch := watch.NewFake()
		dyn.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
			return true, fakeWatch, nil
		})

		eventCh := make(chan ResourceEvent, 10)
		filter := NewNamespaceFilter(NamespaceFilterConfig{IncludePatterns: []string{"prod-*"}})
		inf := NewInformer(dyn, gvr, eventCh, make(chan ResyncEvent, 10), filter, slog.Default())
		inf.listRV = "1"

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			inf.watch(ctx)
		}()
		synctest.Wait()

		denied := makePod("uid-denied", "p", "default", "1")
		fakeWatch.Modify(denied)
		fakeWatch.Delete(denied)
		allowed := makePod("uid-ok", "p", "prod-us", "2")
		fakeWatch.Modify(allowed)
		synctest.Wait()

		ev := <-eventCh
		if ev.Op != EventUpdate || string(ev.Resource.GetUID()) != "uid-ok" {
			t.Fatalf("expected allowed modify, got op=%v uid=%s", ev.Op, ev.Resource.GetUID())
		}

		cancel()
		synctest.Wait()
		<-done
	})
}

func TestGenericInformer_Run_BackoffThenCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		dyn.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("list failed")
		})

		eventCh := make(chan ResourceEvent, 10)
		inf := NewInformer(dyn, gvr, eventCh, make(chan ResyncEvent, 10), nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			inf.Run(ctx)
		}()
		synctest.Wait() // first list fails, then blocks in backoff
		if inf.retries < 1 {
			t.Fatalf("retries = %d, want >= 1", inf.retries)
		}
		cancel() // cancel during backoff wait
		synctest.Wait()
		<-done
	})
}

func TestGenericInformer_Run_BackoffThenSucceed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		lists := 0
		dyn.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			lists++
			if lists == 1 {
				return true, nil, errors.New("transient list failure")
			}
			list := &unstructured.UnstructuredList{}
			list.Object = map[string]any{"metadata": map[string]any{"resourceVersion": "1"}}
			return true, list, nil
		})
		fakeWatch := watch.NewFake()
		dyn.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
			return true, fakeWatch, nil
		})

		eventCh := make(chan ResourceEvent, 10)
		resyncCh := make(chan ResyncEvent, 10)
		inf := NewInformer(dyn, gvr, eventCh, resyncCh, nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			inf.Run(ctx)
		}()
		synctest.Wait()
		time.Sleep(2 * time.Second) // finish backoff
		synctest.Wait()
		if !inf.initialized.Load() {
			t.Fatal("expected informer to initialize after successful retry")
		}
		cancel()
		synctest.Wait()
		<-done
	})
}

func TestRunContinuous_ImmediateReconcileAfterThrottleWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		disc := newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		}})
		dyn := newFakeDynamicClient(gvr, crdGVR)
		mgr := NewInformerManager(dyn, disc, make(chan ResourceEvent, 100), make(chan ResyncEvent, 100), nil, nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			// Zero interval forces the immediate-reconcile branches for both
			// startup CRD resync and subsequent CRD watch events.
			mgr.runContinuous(ctx, nil, nil, 0)
		}()
		awaitRunContinuousReady()
		synctest.Wait()

		crd := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apiextensions.k8s.io/v1",
			"kind":       "CustomResourceDefinition",
			"metadata": map[string]any{
				"name":            "gadgets.example.com",
				"uid":             "crd-uid-2",
				"resourceVersion": "1",
			},
		}}
		if _, err := dyn.Resource(crdGVR).Create(context.Background(), crd, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create crd: %v", err)
		}
		synctest.Wait()

		if _, ok := mgr.stoppers[gvr]; !ok {
			t.Fatal("expected pods informer to remain after immediate reconcile")
		}

		cancel()
		synctest.Wait()
		<-done
	})
}

func TestRunContinuous_ThrottleTimerFires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		disc := newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		}})
		dyn := newFakeDynamicClient(gvr, crdGVR)
		mgr := NewInformerManager(dyn, disc, make(chan ResourceEvent, 100), make(chan ResyncEvent, 100), nil, nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			mgr.RunContinuous(ctx, nil, nil)
		}()
		awaitRunContinuousReady()
		synctest.Wait()

		// Startup CRD resync schedules a throttled reconcile timer. Advancing
		// 10s should fire that timer path.
		time.Sleep(10 * time.Second)
		synctest.Wait()

		if _, ok := mgr.stoppers[gvr]; !ok {
			t.Fatal("expected pods informer after timer reconcile")
		}

		cancel()
		synctest.Wait()
		<-done
	})
}

func TestRunContinuous_DuplicateCRDEventsWhilePending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		disc := newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		}})
		dyn := newFakeDynamicClient(gvr, crdGVR)
		mgr := NewInformerManager(dyn, disc, make(chan ResourceEvent, 100), make(chan ResyncEvent, 100), nil, nil, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			mgr.RunContinuous(ctx, nil, nil)
		}()
		awaitRunContinuousReady()
		synctest.Wait()

		for i, name := range []string{"a.example.com", "b.example.com"} {
			crd := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "apiextensions.k8s.io/v1",
				"kind":       "CustomResourceDefinition",
				"metadata": map[string]any{
					"name":            name,
					"uid":             fmt.Sprintf("crd-dup-%d", i),
					"resourceVersion": fmt.Sprintf("%d", i+1),
				},
			}}
			if _, err := dyn.Resource(crdGVR).Create(context.Background(), crd, metav1.CreateOptions{}); err != nil {
				t.Fatalf("create crd: %v", err)
			}
			synctest.Wait()
		}

		time.Sleep(10 * time.Second)
		synctest.Wait()

		cancel()
		synctest.Wait()
		<-done
	})
}

// fakeDiscoveryPartial optionally returns an error from ServerPreferredResources,
// with either partial results or a nil list.
type fakeDiscoveryPartial struct {
	*fakeDiscoveryWithPreferred
	err          error
	nilResources bool
}

func (f *fakeDiscoveryPartial) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	if f.nilResources {
		return nil, f.err
	}
	res, _ := f.fakeDiscoveryWithPreferred.ServerPreferredResources()
	return res, f.err
}
