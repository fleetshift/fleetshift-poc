package kubernetes

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ObjectResourceType is the single [domain.ResourceType] used to
// report every watched Kubernetes object, regardless of GVR or kind.
// One generic type means new CRDs and API versions never require
// registering a new FleetShift resource type; GVR, kind, namespace,
// and name are instead carried in the object's resource name, labels,
// and observation payload. Built from [AddonID] rather than a
// separate literal, so its service-name prefix can never drift from
// the addon-ownership rule the platform validates against.
const ObjectResourceType domain.ResourceType = domain.ResourceType(AddonID) + "/Object"

// Resource-name path collection IDs for Kubernetes object inventory.
// Together they form:
//
//	{TargetCollectionID}/{clusterResourceID}/{APIResourceCollectionID}/{gvrKey}/{ObjectCollectionID}/{uid}
//
// where clusterResourceID is [KubernetesObjectIdentity.ClusterResourceName].ID().
// ObjectCollectionID is also the schema CollectionID in [InventorySchema].
const (
	TargetCollectionID      domain.CollectionID = "clusters"
	APIResourceCollectionID domain.CollectionID = "apiResources"
	ObjectCollectionID      domain.CollectionID = "objects"
)

// KubernetesObjectIdentity carries the fields needed to compute a
// watched Kubernetes object's [domain.ResourceName], query labels, and
// observation payload. All three are derived from the same identity
// so they never disagree about which object they describe.
//
// ClusterResourceName is the managed cluster resource (e.g.
// "clusters/c1") whose ID becomes the object-name parent segment.
type KubernetesObjectIdentity struct {
	ClusterResourceName domain.ResourceName
	GVR                 schema.GroupVersionResource
	Kind                string
	Namespace           string
	Name                string
	UID                 string
}

// objectScope reports "namespaced" or "cluster" for namespace. Scope
// is fully determined by namespace presence, so it is derived here
// rather than carried as a separate field on
// [KubernetesObjectIdentity]: a namespaced object always has a
// namespace, and a cluster-scoped object never does.
func objectScope(namespace string) string {
	if namespace == "" {
		return "cluster"
	}
	return "namespaced"
}

// GVRKey returns a stable, slash-free key for gvr:
// "{groupKey}~{version}~{resource}", where groupKey is "core" for the
// core API group and the raw group otherwise. This is used for the
// [APIResourceCollectionID] resource-name segment and the k8s.gvr
// label. Neither the raw "group/version/resource" form nor a dotted
// "group.version.resource" form work as a single resource-name
// segment: "/" cannot appear inside one segment, and groups already
// contain dots, which would make a dotted key ambiguous to split back
// apart.
func GVRKey(gvr schema.GroupVersionResource) string {
	groupKey := gvr.Group
	if groupKey == "" {
		groupKey = "core"
	}
	return groupKey + "~" + gvr.Version + "~" + gvr.Resource
}

// encodeResourceNameSegment makes s safe to use as one dynamic segment
// of a [domain.ResourceName]. Dynamic values such as cluster resource
// IDs are not restricted from containing "/", so a value like
// "prod/us-east-1" would otherwise silently insert an extra path
// segment and shift everything after it. url.PathEscape is
// deterministic and collision-free, and leaves "~" -- the [GVRKey]
// separator -- untouched.
func encodeResourceNameSegment(s string) string {
	return url.PathEscape(s)
}

// ObjectResourceName returns the canonical Kubernetes object resource
// name:
// "{TargetCollectionID}/{clusterResourceID}/{APIResourceCollectionID}/{gvrKey}/{ObjectCollectionID}/{uid}".
// clusterResourceID is taken from [KubernetesObjectIdentity.ClusterResourceName].ID()
// so object parents match the managed cluster resource (e.g. clusters/c1). Every dynamic
// segment is path-encoded before being joined, and the result is built
// with [domain.ParseResourceName] rather than cast from a raw string, so
// a malformed identity (e.g. an empty UID) fails here instead of
// producing an invalid name downstream. Scoping by cluster and GVR
// gives same-process resync a natural collection boundary; keying the
// leaf by UID rather than namespace/name means deleting and recreating
// an object under the same namespace/name is correctly treated as a new
// incarnation rather than an overwrite.
func ObjectResourceName(id KubernetesObjectIdentity) (domain.ResourceName, error) {
	if err := requireClusterResourceName(id.ClusterResourceName); err != nil {
		return "", fmt.Errorf("kubernetes object resource name (cluster %q, gvr %q, uid %q): %w",
			id.ClusterResourceName, GVRKey(id.GVR), id.UID, err)
	}
	clusterID := encodeResourceNameSegment(string(id.ClusterResourceName.ID()))
	name, err := domain.ParseResourceName(
		string(TargetCollectionID) + "/" + clusterID +
			"/" + string(APIResourceCollectionID) + "/" + encodeResourceNameSegment(GVRKey(id.GVR)) +
			"/" + string(ObjectCollectionID) + "/" + encodeResourceNameSegment(id.UID),
	)
	if err != nil {
		return "", fmt.Errorf("kubernetes object resource name (cluster %q, gvr %q, uid %q): %w",
			id.ClusterResourceName, GVRKey(id.GVR), id.UID, err)
	}
	return name, nil
}

// ObjectCollectionName returns the exact inventory collection for
// clusterResourceName + gvr:
// "{TargetCollectionID}/{clusterResourceID}/{APIResourceCollectionID}/{gvrKey}/{ObjectCollectionID}".
// This matches [ObjectResourceName]'s parent collection.
func ObjectCollectionName(clusterResourceName domain.ResourceName, gvr schema.GroupVersionResource) (domain.CollectionName, error) {
	if err := requireClusterResourceName(clusterResourceName); err != nil {
		return "", fmt.Errorf("kubernetes object collection name (cluster %q, gvr %q): %w", clusterResourceName, GVRKey(gvr), err)
	}
	clusterID := encodeResourceNameSegment(string(clusterResourceName.ID()))
	name, err := domain.ParseCollectionName(
		string(TargetCollectionID) + "/" + clusterID +
			"/" + string(APIResourceCollectionID) + "/" + encodeResourceNameSegment(GVRKey(gvr)) +
			"/" + string(ObjectCollectionID),
	)
	if err != nil {
		return "", fmt.Errorf("kubernetes object collection name (cluster %q, gvr %q): %w", clusterResourceName, GVRKey(gvr), err)
	}
	return name, nil
}

// ParseClusterResourceName parses an untrusted string into a flat
// managed-cluster resource name (clusters/{id}). Use this at string
// boundaries (e.g. target properties). Callers that already hold a
// [domain.ResourceName] should use [requireClusterResourceName] instead
// of casting to string and re-parsing.
func ParseClusterResourceName(s string) (domain.ResourceName, error) {
	name, err := domain.ParseResourceName(s)
	if err != nil {
		return "", fmt.Errorf("cluster resource name: %w", err)
	}
	if err := requireClusterResourceName(name); err != nil {
		return "", err
	}
	return name, nil
}

// requireClusterResourceName checks the kubernetes-specific constraint that
// name is under [TargetCollectionID] (flat clusters/{id}). It trusts
// structural resource-name validity to whoever produced the typed value
// ([domain.ParseResourceName] / [ParseClusterResourceName]).
func requireClusterResourceName(name domain.ResourceName) error {
	if name.Collection() != domain.CollectionName(TargetCollectionID) {
		return fmt.Errorf("%w: cluster resource name %q must be under %q", domain.ErrInvalidArgument, name, TargetCollectionID)
	}
	return nil
}

// ObjectLabels returns the initial set of Kubernetes identity labels
// for id: GVR, kind, scope, namespace, name, and UID, for filtering and
// grouping. Values are stored unencoded -- unlike [ObjectResourceName]'s
// path segments, labels are not part of a resource-name path, so there
// is nothing to escape. k8s.namespace is omitted entirely for
// cluster-scoped objects rather than set to "", so its mere presence
// signals namespace scope. Kubernetes' user-defined object labels are
// deliberately not copied here: they are high-cardinality and
// uncontrolled, and belong in the observation payload instead of the
// shared label index.
func ObjectLabels(id KubernetesObjectIdentity) map[string]string {
	labels := map[string]string{
		"k8s.gvr":      GVRKey(id.GVR),
		"k8s.group":    id.GVR.Group,
		"k8s.version":  id.GVR.Version,
		"k8s.resource": id.GVR.Resource,
		"k8s.kind":     id.Kind,
		"k8s.scope":    objectScope(id.Namespace),
		"k8s.name":     id.Name,
		"k8s.uid":      id.UID,
	}
	if id.Namespace != "" {
		labels["k8s.namespace"] = id.Namespace
	}
	return labels
}

// ObjectObservation returns the base observation payload for id: the
// Kubernetes API identity it was watched through (group/version/
// resource/scope, taken from id since obj's own apiVersion/kind
// strings don't carry resource plural or scope), the object's own
// apiVersion/kind/metadata as observed, and extracted (the
// schema-hook-computed enrichment for this object's kind, or an empty
// object when none applies). extracted is passed through opaquely;
// this function does not interpret it.
//
// deletionTimestamp and ownerReferences are included alongside the
// other metadata fields even though they are absent from most objects:
// both are generic, always-queryable-when-present object metadata
// (deletion in progress; who owns this object) rather than per-kind
// enrichment, so they belong here rather than in extracted.
//
// A marshal failure here would mean extracted contains a value JSON
// cannot represent (a func, chan, or complex number) -- something a
// well-behaved extraction hook never produces. Rather than panic on
// that, or change this function's return type just to plumb the
// error somewhere, an empty JSON object is returned so one malformed
// hook degrades a single observation instead of the whole indexer.
func ObjectObservation(id KubernetesObjectIdentity, obj *unstructured.Unstructured, extracted map[string]any) json.RawMessage {
	if extracted == nil {
		extracted = map[string]any{}
	}
	observation := map[string]any{
		"gvr": map[string]any{
			"group":    id.GVR.Group,
			"version":  id.GVR.Version,
			"resource": id.GVR.Resource,
			"scope":    objectScope(id.Namespace),
		},
		"apiVersion": obj.GetAPIVersion(),
		"kind":       obj.GetKind(),
		"metadata": map[string]any{
			"uid":               string(obj.GetUID()),
			"namespace":         obj.GetNamespace(),
			"name":              obj.GetName(),
			"resourceVersion":   obj.GetResourceVersion(),
			"generation":        obj.GetGeneration(),
			"creationTimestamp": obj.GetCreationTimestamp(),
			"deletionTimestamp": obj.GetDeletionTimestamp(),
			"labels":            obj.GetLabels(),
			"annotations":       obj.GetAnnotations(),
			"ownerReferences":   obj.GetOwnerReferences(),
		},
		"extracted": extracted,
	}
	raw, err := json.Marshal(observation)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

// RawCondition is one Kubernetes status.conditions entry as read from
// an unstructured object, before projection into a [domain.Condition].
type RawCondition struct {
	Type    string
	Status  string
	Reason  string
	Message string

	// LastTransitionTime is the raw RFC 3339 timestamp string as
	// encoded by Kubernetes. Empty or unparsable values fall back to
	// the observedAt passed to [ObjectConditions].
	LastTransitionTime string
}

// ObjectConditions projects raw Kubernetes status.conditions into
// FleetShift conditions: the complete latest set is reported, using
// the Kubernetes condition type directly with no prefix. Conditions
// with an empty type are dropped, since they cannot be a valid
// [domain.ConditionType]. Only the standard True/False/Unknown
// statuses are kept; a condition using a nonstandard status (some
// CRDs report free-form strings instead) is dropped rather than
// guessed at. A missing or unparsable LastTransitionTime falls back
// to observedAt. When multiple source conditions share a type --
// which should not happen, but is not validated by the Kubernetes
// API -- the last one wins, matching a map upsert. Cross-kind
// interpretation, such as synthesizing rollup health from condition
// names, is deliberately out of scope here: that belongs to per-kind
// enrichment, not this generic projection.
func ObjectConditions(raw []RawCondition, observedAt time.Time) []domain.Condition {
	order := make([]domain.ConditionType, 0, len(raw))
	byType := make(map[domain.ConditionType]domain.Condition, len(raw))

	for _, rc := range raw {
		ct, err := domain.NewConditionType(rc.Type)
		if err != nil {
			continue
		}
		status, err := domain.ParseConditionStatus(rc.Status)
		if err != nil {
			continue
		}
		transition := observedAt
		if rc.LastTransitionTime != "" {
			if t, err := time.Parse(time.RFC3339, rc.LastTransitionTime); err == nil {
				transition = t
			}
		}
		cond, err := domain.NewCondition(ct, status, rc.Reason, rc.Message, transition)
		if err != nil {
			continue
		}
		if _, seen := byType[ct]; !seen {
			order = append(order, ct)
		}
		byType[ct] = cond
	}

	result := make([]domain.Condition, len(order))
	for i, ct := range order {
		result[i] = byType[ct]
	}
	return result
}
