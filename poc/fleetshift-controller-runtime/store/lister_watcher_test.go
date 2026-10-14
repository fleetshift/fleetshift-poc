package store

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

func TestListerWatcherListAndWatch(t *testing.T) {
	s := New(testScheme(t))
	gvk := testGV.WithKind("Widget")
	lw := NewListerWatcher(s, gvk)

	if err := s.Create(&Widget{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns"}}); err != nil {
		t.Fatal(err)
	}

	listObj, err := lw.List(metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	list := listObj.(*WidgetList)
	if len(list.Items) != 1 || list.ResourceVersion == "" {
		t.Fatalf("list = %+v", list)
	}

	w, err := lw.Watch(metav1.ListOptions{ResourceVersion: list.ResourceVersion})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	if err := s.Create(&Widget{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"}}); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-w.ResultChan():
		if ev.Type != watch.Added {
			t.Fatalf("type %v", ev.Type)
		}
		if ev.Object.(*Widget).Name != "b" {
			t.Fatalf("name %q", ev.Object.(*Widget).Name)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestListerWatcherOptsOutOfWatchList(t *testing.T) {
	// client-go ≥1.35 defaults WatchListClient=true. Without this opt-out,
	// Reflector uses streaming WatchList and waits forever for a Bookmark
	// our store never emits (HasSynced never becomes true).
	lw := NewListerWatcher(New(testScheme(t)), testGV.WithKind("Widget"))
	type unsupported interface{ IsWatchListSemanticsUnSupported() bool }
	u, ok := lw.(unsupported)
	if !ok || !u.IsWatchListSemanticsUnSupported() {
		t.Fatal("ListerWatcher must opt out of WatchList semantics")
	}
}

func TestListerWatcherDrivesStockReflector(t *testing.T) {
	s := New(testScheme(t))
	gvk := testGV.WithKind("Widget")
	lw := NewListerWatcher(s, gvk)

	informer := cache.NewSharedIndexInformer(lw, &Widget{}, 0, cache.Indexers{})
	added := make(chan string, 4)
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			w := obj.(*Widget)
			added <- w.Name
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Bound sync tightly: WatchList-without-bookmarks hangs for minutes.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go informer.RunWithContext(ctx)

	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		t.Fatal("informer failed to sync within 2s (missing WatchList opt-out?)")
	}

	if err := s.Create(&Widget{ObjectMeta: metav1.ObjectMeta{Name: "from-reflector", Namespace: "ns"}}); err != nil {
		t.Fatal(err)
	}

	select {
	case name := <-added:
		if name != "from-reflector" {
			t.Fatalf("got %q", name)
		}
	case <-ctx.Done():
		t.Fatal("Reflector did not deliver Added event")
	}
}
