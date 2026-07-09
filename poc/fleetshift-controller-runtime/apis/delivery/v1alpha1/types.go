// Package v1alpha1 defines the Delivery CR that controllers reconcile.
// Desired state arrives via the FleetShift delivery contract and is
// projected into these objects by the provider; status updates flow
// back out as DeliveryReporter calls.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	runtimescheme "sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the API group/version for Delivery resources.
	GroupVersion = schema.GroupVersion{Group: "delivery.fleetshift.io", Version: "v1alpha1"}

	// SchemeBuilder registers Delivery types.
	SchemeBuilder = &runtimescheme.Builder{GroupVersion: GroupVersion}

	// Scheme is a scheme with Delivery types registered.
	Scheme = runtime.NewScheme()
)

func init() {
	SchemeBuilder.Register(&Delivery{}, &DeliveryList{})
	utilruntime.Must(SchemeBuilder.AddToScheme(Scheme))
	metav1.AddToGroupVersion(Scheme, GroupVersion)
}

// Delivery is the controller-runtime view of a FleetShift delivery.
// Spec is desired state from the platform; Status is observed state
// written by the reconciler and mirrored back via DeliveryReporter.
type Delivery struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              DeliverySpec   `json:"spec,omitempty"`
	Status            DeliveryStatus `json:"status,omitempty"`
}

// DeliverySpec is the desired delivery intent.
type DeliverySpec struct {
	// DeliveryID is the platform delivery identifier.
	DeliveryID string `json:"deliveryID"`
	// FulfillmentID is the parent fulfillment.
	FulfillmentID string `json:"fulfillmentID,omitempty"`
	// TargetID is the delivery target (also the multicluster cluster name).
	TargetID string `json:"targetID"`
	// Generation is the fulfillment generation for stale-delivery fencing.
	Generation int64 `json:"generation"`
	// Operation is "deliver" or "remove".
	Operation string `json:"operation"`
	// ManifestType is the opaque dispatch label.
	ManifestType string `json:"manifestType,omitempty"`
	// ManifestJSON is the raw manifest payload.
	ManifestJSON string `json:"manifestJSON,omitempty"`
	// AuthToken is the caller's passthrough JWT (POC only; real systems
	// would keep this out of the CR and in a secret/journal).
	AuthToken string `json:"authToken,omitempty"`
}

// DeliveryStatus is the observed delivery state.
type DeliveryStatus struct {
	// Phase mirrors contract.DeliveryState.
	Phase string `json:"phase,omitempty"`
	// Message is a human-readable progress or error string.
	Message string `json:"message,omitempty"`
	// ObservedGeneration is the last Spec.Generation the reconciler acted on.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Reported marks that a terminal ReportResult was sent to the platform.
	Reported bool `json:"reported,omitempty"`
}

// DeliveryList is a list of Delivery.
type DeliveryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Delivery `json:"items"`
}

func (in *Delivery) DeepCopyObject() runtime.Object {
	out := new(Delivery)
	in.DeepCopyInto(out)
	return out
}

func (in *Delivery) DeepCopyInto(out *Delivery) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	out.Status = in.Status
}

func (in *DeliveryList) DeepCopyObject() runtime.Object {
	out := new(DeliveryList)
	in.DeepCopyInto(out)
	return out
}

func (in *DeliveryList) DeepCopyInto(out *DeliveryList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Delivery, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}
