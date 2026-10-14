package kubernetes

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ObjectResourceType is the single [domain.ResourceType] used to
// report every watched Kubernetes object, regardless of GVR or kind.
// One generic type means new CRDs and API versions never require
// registering a new FleetShift resource type. Object identity is split
// across the resource name (cluster, optional namespace, versionless
// group-resource, UID) and the observation payload (apiVersion, kind,
// metadata.name, and related fields). Built from [AddonID] rather than
// a separate literal, so its service-name prefix can never drift from
// the addon-ownership rule the platform validates against.
const ObjectResourceType domain.ResourceType = domain.ResourceType(AddonID) + "/Object"

// Resource-name path collection IDs for Kubernetes object inventory.
// Namespaced objects use:
//
//	{ClusterCollectionID}/{clusterResourceID}/{NamespaceCollectionID}/{namespace}/{APIResourceCollectionID}/{groupResourceKey}/{ObjectCollectionID}/{uid}
//
// Cluster-scoped objects omit the namespace branch:
//
//	{ClusterCollectionID}/{clusterResourceID}/{APIResourceCollectionID}/{groupResourceKey}/{ObjectCollectionID}/{uid}
//
// where clusterResourceID is [KubernetesObjectIdentity.ClusterResourceName].ID().
// ObjectCollectionID is also the schema CollectionID in [InventorySchema].
const (
	// ClusterCollectionID is the managed-cluster parent collection ("clusters").
	ClusterCollectionID domain.CollectionID = "clusters"
	// NamespaceCollectionID is the Kubernetes namespace branch ("namespaces").
	NamespaceCollectionID domain.CollectionID = "namespaces"
	// APIResourceCollectionID is the versionless group-resource branch ("apiResources").
	APIResourceCollectionID domain.CollectionID = "apiResources"
	// ObjectCollectionID is the object leaf collection ("objects").
	ObjectCollectionID domain.CollectionID = "objects"
)

// ObjectScope is the discovery-authoritative scope of a Kubernetes
// API resource. It selects the namespaced vs cluster-scoped resource
// name pattern and must never be inferred from metadata.namespace.
type ObjectScope string

const (
	// ObjectScopeUnknown is the fail-closed zero value. It is never a
	// valid scope for name construction or event processing.
	ObjectScopeUnknown ObjectScope = ""
	// ObjectScopeNamespaced means discovery reported APIResource.Namespaced=true.
	ObjectScopeNamespaced ObjectScope = "namespaced"
	// ObjectScopeCluster means discovery reported APIResource.Namespaced=false.
	ObjectScopeCluster ObjectScope = "cluster"
)

// ParseObjectScope parses a discovery scope spelling ("namespaced" or
// "cluster"). Empty and unknown values are rejected. Callers must not
// invent scope from object metadata.
func ParseObjectScope(s string) (ObjectScope, error) {
	switch ObjectScope(s) {
	case ObjectScopeNamespaced, ObjectScopeCluster:
		return ObjectScope(s), nil
	case ObjectScopeUnknown:
		return "", fmt.Errorf("%w: object scope is required", domain.ErrInvalidArgument)
	default:
		return "", fmt.Errorf("%w: invalid object scope %q", domain.ErrInvalidArgument, s)
	}
}

// ScopeNamespace is a discovery [ObjectScope] bound to the object
// namespace that is legal under that scope. Namespaced scope requires a
// non-empty namespace; cluster scope requires an empty namespace.
// Construct only via [NewScopeNamespace].
type ScopeNamespace struct {
	scope     ObjectScope
	namespace string
}

// NewScopeNamespace returns a concrete [ScopeNamespace].
// scope must be [ObjectScopeNamespaced] or [ObjectScopeCluster];
// namespace must be non-empty for namespaced scope and empty for cluster scope.
func NewScopeNamespace(scope ObjectScope, namespace string) (ScopeNamespace, error) {
	scope, err := ParseObjectScope(string(scope))
	if err != nil {
		return ScopeNamespace{}, err
	}
	switch scope {
	case ObjectScopeNamespaced:
		if namespace == "" {
			return ScopeNamespace{}, fmt.Errorf("%w: namespaced object requires a non-empty namespace", domain.ErrInvalidArgument)
		}
		return ScopeNamespace{scope: scope, namespace: namespace}, nil
	case ObjectScopeCluster:
		if namespace != "" {
			return ScopeNamespace{}, fmt.Errorf("%w: cluster-scoped object must have an empty namespace, got %q", domain.ErrInvalidArgument, namespace)
		}
		return ScopeNamespace{scope: scope, namespace: ""}, nil
	default:
		return ScopeNamespace{}, fmt.Errorf("%w: invalid object scope %q", domain.ErrInvalidArgument, scope)
	}
}

// KubernetesObjectIdentity carries the fields needed to compute a
// watched Kubernetes object's [domain.ResourceName] and observation
// payload. Both are derived from the same identity so they never
// disagree about which object they describe.
type KubernetesObjectIdentity struct {
	// ClusterResourceName is the managed cluster (e.g. "clusters/c1")
	// whose ID becomes the object-name parent segment.
	ClusterResourceName domain.ResourceName
	// GVR is the API resource the object was watched through. Only the
	// group/resource pair is used for naming; version is observation-only.
	GVR schema.GroupVersionResource
	// ScopeNamespace is the discovery scope bound to metadata.namespace.
	// Construct via [NewScopeNamespace]; the zero value is rejected by naming.
	ScopeNamespace ScopeNamespace
	// Kind is the object's Kubernetes kind (observation metadata).
	Kind string
	// Name is the object's metadata.name (observation metadata; not a name path segment).
	Name string
	// UID is the object's metadata.uid and the inventory resource-name leaf.
	UID string
}

// GroupResourceKey returns the canonical versionless key for gr:
// "{resource}" for the core API group and "{resource}.{group}" for a
// named group. This matches schema.GroupResource.String() and is used
// for the [APIResourceCollectionID] resource-name segment. API version
// is intentionally omitted so a preferred-version change does not
// rename inventory rows.
func GroupResourceKey(gr schema.GroupResource) (string, error) {
	if gr.Resource == "" {
		return "", fmt.Errorf("%w: group resource key requires a non-empty resource", domain.ErrInvalidArgument)
	}
	if strings.Contains(gr.Resource, "/") {
		return "", fmt.Errorf("%w: group resource key rejects subresource %q", domain.ErrInvalidArgument, gr.Resource)
	}
	if strings.Contains(gr.Resource, ".") {
		return "", fmt.Errorf("%w: group resource key rejects '.' in resource %q", domain.ErrInvalidArgument, gr.Resource)
	}
	key := gr.String()
	if _, err := ParseGroupResourceKey(key); err != nil {
		return "", err
	}
	return key, nil
}

// ParseGroupResourceKey parses a [GroupResourceKey] spelling and
// requires a canonical round-trip (rejects trailing dots and other
// non-canonical forms).
func ParseGroupResourceKey(key string) (schema.GroupResource, error) {
	if key == "" {
		return schema.GroupResource{}, fmt.Errorf("%w: empty group resource key", domain.ErrInvalidArgument)
	}
	if strings.HasPrefix(key, ".") || strings.HasSuffix(key, ".") {
		return schema.GroupResource{}, fmt.Errorf("%w: non-canonical group resource key %q", domain.ErrInvalidArgument, key)
	}
	gr := schema.ParseGroupResource(key)
	if gr.Resource == "" {
		return schema.GroupResource{}, fmt.Errorf("%w: group resource key %q has empty resource", domain.ErrInvalidArgument, key)
	}
	if strings.Contains(gr.Resource, "/") {
		return schema.GroupResource{}, fmt.Errorf("%w: group resource key rejects subresource %q", domain.ErrInvalidArgument, gr.Resource)
	}
	if strings.Contains(gr.Resource, ".") {
		return schema.GroupResource{}, fmt.Errorf("%w: group resource key rejects '.' in resource %q", domain.ErrInvalidArgument, gr.Resource)
	}
	if gr.String() != key {
		return schema.GroupResource{}, fmt.Errorf("%w: non-canonical group resource key %q", domain.ErrInvalidArgument, key)
	}
	return gr, nil
}

// encodeResourceNameSegment makes s safe to use as one dynamic segment
// of a [domain.ResourceName]. Dynamic values such as cluster resource
// IDs are not restricted from containing "/", so a value like
// "prod/us-east-1" would otherwise silently insert an extra path
// segment and shift everything after it. url.PathEscape is
// deterministic and collision-free.
func encodeResourceNameSegment(s string) string {
	return url.PathEscape(s)
}

// ObjectResourceName returns the canonical Kubernetes object resource
// name. The leaf is always the object UID so delete/recreate under the
// same namespace/name is a new inventory row. Requires a non-zero
// [ScopeNamespace] and a flat [ClusterCollectionID] parent.
//
//	namespaced: clusters/{cluster}/namespaces/{ns}/apiResources/{grKey}/objects/{uid}
//	cluster:    clusters/{cluster}/apiResources/{grKey}/objects/{uid}
func ObjectResourceName(id KubernetesObjectIdentity) (domain.ResourceName, error) {
	if err := requireClusterResourceName(id.ClusterResourceName); err != nil {
		return "", fmt.Errorf("kubernetes object resource name (cluster %q, uid %q): %w",
			id.ClusterResourceName, id.UID, err)
	}
	if id.ScopeNamespace == (ScopeNamespace{}) {
		return "", fmt.Errorf("kubernetes object resource name (cluster %q, uid %q): %w: object scope is required",
			id.ClusterResourceName, id.UID, domain.ErrInvalidArgument)
	}
	grKey, err := GroupResourceKey(id.GVR.GroupResource())
	if err != nil {
		return "", fmt.Errorf("kubernetes object resource name (cluster %q, uid %q): %w",
			id.ClusterResourceName, id.UID, err)
	}
	clusterID := encodeResourceNameSegment(string(id.ClusterResourceName.ID()))
	path := string(ClusterCollectionID) + "/" + clusterID
	if id.ScopeNamespace.scope == ObjectScopeNamespaced {
		path += "/" + string(NamespaceCollectionID) + "/" + encodeResourceNameSegment(id.ScopeNamespace.namespace)
	}
	path += "/" + string(APIResourceCollectionID) + "/" + encodeResourceNameSegment(grKey) +
		"/" + string(ObjectCollectionID) + "/" + encodeResourceNameSegment(id.UID)
	name, err := domain.ParseResourceName(path)
	if err != nil {
		return "", fmt.Errorf("kubernetes object resource name (cluster %q, gr %q, uid %q): %w",
			id.ClusterResourceName, grKey, id.UID, err)
	}
	return name, nil
}

// ObjectCollectionName returns the parent collection of
// [ObjectResourceName] for clusterResourceName + scopeNamespace + gvr
// (everything except the UID leaf). Requires a non-zero [ScopeNamespace].
func ObjectCollectionName(clusterResourceName domain.ResourceName, scopeNamespace ScopeNamespace, gvr schema.GroupVersionResource) (domain.CollectionName, error) {
	if err := requireClusterResourceName(clusterResourceName); err != nil {
		return "", fmt.Errorf("kubernetes object collection name (cluster %q): %w", clusterResourceName, err)
	}
	if scopeNamespace == (ScopeNamespace{}) {
		return "", fmt.Errorf("kubernetes object collection name (cluster %q): %w: object scope is required",
			clusterResourceName, domain.ErrInvalidArgument)
	}
	grKey, err := GroupResourceKey(gvr.GroupResource())
	if err != nil {
		return "", fmt.Errorf("kubernetes object collection name (cluster %q): %w", clusterResourceName, err)
	}
	clusterID := encodeResourceNameSegment(string(clusterResourceName.ID()))
	path := string(ClusterCollectionID) + "/" + clusterID
	if scopeNamespace.scope == ObjectScopeNamespaced {
		path += "/" + string(NamespaceCollectionID) + "/" + encodeResourceNameSegment(scopeNamespace.namespace)
	}
	path += "/" + string(APIResourceCollectionID) + "/" + encodeResourceNameSegment(grKey) +
		"/" + string(ObjectCollectionID)
	name, err := domain.ParseCollectionName(path)
	if err != nil {
		return "", fmt.Errorf("kubernetes object collection name (cluster %q, gr %q): %w", clusterResourceName, grKey, err)
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
// name is under [ClusterCollectionID] (flat clusters/{id}). It trusts
// structural resource-name validity to whoever produced the typed value
// ([domain.ParseResourceName] / [ParseClusterResourceName]).
func requireClusterResourceName(name domain.ResourceName) error {
	if name.Collection() != domain.CollectionName(ClusterCollectionID) {
		return fmt.Errorf("%w: cluster resource name %q must be under %q", domain.ErrInvalidArgument, name, ClusterCollectionID)
	}
	return nil
}

// ObjectLabels projects the object's metadata.labels into FleetShift
// localLabels: the complete latest set, keys and values unchanged.
// This mirrors [ObjectConditions] lifting status.conditions into the
// inventory envelope. Nil or empty source labels become an empty map
// so a ReplaceBatch clears any prior localLabels. Object identity
// (GVR, kind, namespace, name, UID) lives on the resource name and
// observation payload, not here.
func ObjectLabels(obj *unstructured.Unstructured) map[string]string {
	src := obj.GetLabels()
	if len(src) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(src))
	maps.Copy(out, src)
	return out
}

// ObjectObservation returns the base observation payload for id: the
// Kubernetes API identity it was watched through (group/version/
// resource from id.GVR and scope from id.ScopeNamespace — obj's own
// apiVersion/kind strings don't carry resource plural or scope), the
// object's own apiVersion/kind/metadata as observed, and extracted
// (schema-hook enrichment, or an empty object when none applies).
// extracted is passed through opaquely; this function does not
// interpret it.
//
// metadata.labels are omitted here: they are projected into
// [ObjectLabels] / resource.localLabels instead, matching how
// status.conditions are lifted out of the observation blob.
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
			"scope":    string(id.ScopeNamespace.scope),
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
