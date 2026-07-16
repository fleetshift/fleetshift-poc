package store

import (
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

var testGV = schema.GroupVersion{Group: "test.fleetshift.io", Version: "v1"}

type Widget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WidgetSpec `json:"spec,omitempty"`
}

type WidgetSpec struct {
	Value string `json:"value"`
}

func (w *Widget) DeepCopyObject() runtime.Object {
	out := new(Widget)
	w.DeepCopyInto(out)
	return out
}

func (w *Widget) DeepCopyInto(out *Widget) {
	*out = *w
	out.TypeMeta = w.TypeMeta
	w.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = w.Spec
}

type WidgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Widget `json:"items"`
}

func (in *WidgetList) DeepCopyObject() runtime.Object {
	out := new(WidgetList)
	in.DeepCopyInto(out)
	return out
}

func (in *WidgetList) DeepCopyInto(out *WidgetList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Widget, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	s.AddKnownTypes(testGV, &Widget{}, &WidgetList{})
	metav1.AddToGroupVersion(s, testGV)
	return s
}

func TestStoreCreateGetWatch(t *testing.T) {
	s := New(testScheme(t))
	gvk := testGV.WithKind("Widget")

	ch, cancel := s.Watch(gvk)
	defer cancel()

	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Name: "one", Namespace: "default"},
		Spec:       WidgetSpec{Value: "hello"},
	}
	if err := s.Create(w); err != nil {
		t.Fatal(err)
	}

	var got Widget
	if err := s.Get(gvk, "default", "one", &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Value != "hello" {
		t.Fatalf("got value %q", got.Spec.Value)
	}
	if got.ResourceVersion == "" {
		t.Fatal("expected resourceVersion")
	}

	select {
	case ev := <-ch:
		if ev.Type != watch.Added {
			t.Fatalf("event type %v", ev.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("expected Added event")
	}
}

func TestStoreOptimisticConcurrency(t *testing.T) {
	s := New(testScheme(t))
	gvk := testGV.WithKind("Widget")

	w := &Widget{ObjectMeta: metav1.ObjectMeta{Name: "one", Namespace: "ns"}}
	if err := s.Create(w); err != nil {
		t.Fatal(err)
	}
	var current Widget
	if err := s.Get(gvk, "ns", "one", &current); err != nil {
		t.Fatal(err)
	}

	stale := current.DeepCopyObject().(*Widget)
	stale.Spec.Value = "stale"
	current.Spec.Value = "fresh"
	if err := s.Update(&current); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(stale); err == nil {
		t.Fatal("expected conflict on stale resourceVersion")
	}
}

func TestWatchMetaFromResourceVersion(t *testing.T) {
	s := New(testScheme(t))
	gvk := testGV.WithKind("Widget")

	if err := s.Create(&Widget{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns"}}); err != nil {
		t.Fatal(err)
	}
	listObj, err := s.ListMeta(gvk, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	list := listObj.(*WidgetList)
	if list.ResourceVersion == "" {
		t.Fatal("expected list resourceVersion")
	}
	if len(list.Items) != 1 {
		t.Fatalf("list items = %d, want 1", len(list.Items))
	}

	w, err := s.WatchMeta(gvk, metav1.ListOptions{ResourceVersion: list.ResourceVersion})
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
			t.Fatalf("event type %v", ev.Type)
		}
		obj, ok := ev.Object.(*Widget)
		if !ok {
			t.Fatalf("object type %T", ev.Object)
		}
		if obj.Name != "b" {
			t.Fatalf("got name %q, want b (events at/before list RV must be skipped)", obj.Name)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch event")
	}
}

func TestWatchMetaGoneAfterCompact(t *testing.T) {
	s := New(testScheme(t))
	gvk := testGV.WithKind("Widget")

	if err := s.Create(&Widget{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns"}}); err != nil {
		t.Fatal(err)
	}
	listObj, err := s.ListMeta(gvk, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	oldRV := listObj.(*WidgetList).ResourceVersion

	if err := s.Create(&Widget{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"}}); err != nil {
		t.Fatal(err)
	}
	listObj, err = s.ListMeta(gvk, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	newRV := listObj.(*WidgetList).ResourceVersion

	s.Compact(gvk, newRV)

	_, err = s.WatchMeta(gvk, metav1.ListOptions{ResourceVersion: oldRV})
	if err == nil {
		t.Fatal("expected ResourceExpired after compact")
	}
	if !apierrors.IsResourceExpired(err) && !apierrors.IsGone(err) {
		t.Fatalf("error = %v, want ResourceExpired/Gone", err)
	}
}

func TestWatchMetaDoesNotDropEvents(t *testing.T) {
	s := New(testScheme(t))
	gvk := testGV.WithKind("Widget")

	w, err := s.WatchMeta(gvk, metav1.ListOptions{ResourceVersion: "0"})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	const n = 8
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			name := string(rune('a' + i))
			if err := s.Create(&Widget{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}}); err != nil {
				t.Errorf("create: %v", err)
				return
			}
		}
	}()

	got := 0
	deadline := time.After(2 * time.Second)
	for got < n {
		select {
		case <-done:
			// keep draining
		case ev, ok := <-w.ResultChan():
			if !ok {
				t.Fatalf("watch closed early after %d events", got)
			}
			if ev.Type == watch.Added {
				got++
			}
		case <-deadline:
			t.Fatalf("only received %d/%d events (silent drops?)", got, n)
		}
	}
}
