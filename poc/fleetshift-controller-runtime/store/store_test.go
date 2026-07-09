package store

import (
	"testing"

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

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	s.AddKnownTypes(testGV, &Widget{})
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
	default:
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
