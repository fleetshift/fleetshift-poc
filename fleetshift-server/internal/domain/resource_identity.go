package domain

import (
	"fmt"
	"strings"
	"time"
)

// ServiceName identifies the extension service that owns a representation
// (e.g. "kind.fleetshift.io").
type ServiceName string

// NewServiceName validates and returns a [ServiceName]. It rejects empty
// values and values containing '/'.
func NewServiceName(s string) (ServiceName, error) {
	if s == "" {
		return "", fmt.Errorf("service name: %w: must not be empty", ErrInvalidArgument)
	}
	if strings.Contains(s, "/") {
		return "", fmt.Errorf("service name: %w: must not contain '/'", ErrInvalidArgument)
	}
	return ServiceName(s), nil
}

// APIVersion is the version of the extension API surface (e.g. "v1alpha1").
type APIVersion string

// NewAPIVersion validates and returns an [APIVersion]. It rejects empty
// values and values that do not start with 'v'.
func NewAPIVersion(v string) (APIVersion, error) {
	if v == "" {
		return "", fmt.Errorf("api version: %w: must not be empty", ErrInvalidArgument)
	}
	if !strings.HasPrefix(v, "v") {
		return "", fmt.Errorf("api version: %w: must start with 'v'", ErrInvalidArgument)
	}
	return APIVersion(v), nil
}

// CollectionID identifies a resource collection (e.g. "clusters").
type CollectionID string

// NewCollectionID validates and returns a [CollectionID]. It rejects
// empty values, values not starting with a lowercase letter
// (collection identifiers are lowerCamelCase per AIP-122), and values
// containing '/'.
func NewCollectionID(s string) (CollectionID, error) {
	if s == "" {
		return "", fmt.Errorf("collection id: %w: must not be empty", ErrInvalidArgument)
	}
	if s[0] < 'a' || s[0] > 'z' {
		return "", fmt.Errorf("collection id: %w: must start with a lowercase letter (lowerCamelCase)", ErrInvalidArgument)
	}
	if strings.Contains(s, "/") {
		return "", fmt.Errorf("collection id: %w: must not contain '/'", ErrInvalidArgument)
	}
	return CollectionID(s), nil
}

// ResourceID identifies a resource within its parent collection
// (e.g. "prod-us-east-1" in "clusters/prod-us-east-1"). This is
// the "resource ID segment" per AIP-122.
type ResourceID string

// NewResourceID validates and returns a [ResourceID]. It rejects empty
// values and values containing '/'.
func NewResourceID(s string) (ResourceID, error) {
	if s == "" {
		return "", fmt.Errorf("resource id: %w: must not be empty", ErrInvalidArgument)
	}
	if strings.Contains(s, "/") {
		return "", fmt.Errorf("resource id: %w: must not contain '/'", ErrInvalidArgument)
	}
	return ResourceID(s), nil
}

// CollectionName is the full path to a collection container.
// Flat: "clusters". Nested (future): "publishers/123/books".
type CollectionName string

// NewCollectionName constructs a [CollectionName] from a flat
// [CollectionID]. For nested collections, use [ParseCollectionName].
func NewCollectionName(id CollectionID) CollectionName {
	return CollectionName(id)
}

// validateCanonicalPath rejects paths with leading slashes, trailing
// slashes, or empty segments (double slashes). Returns the split
// segments on success.
func validateCanonicalPath(kind, s string) ([]string, error) {
	if s == "" {
		return nil, fmt.Errorf("%s: %w: must not be empty", kind, ErrInvalidArgument)
	}
	if strings.HasPrefix(s, "/") {
		return nil, fmt.Errorf("%s: %w: must not start with '/'", kind, ErrInvalidArgument)
	}
	if strings.HasSuffix(s, "/") {
		return nil, fmt.Errorf("%s: %w: must not end with '/'", kind, ErrInvalidArgument)
	}
	parts := strings.Split(s, "/")
	for _, p := range parts {
		if p == "" {
			return nil, fmt.Errorf("%s: %w: must not contain empty segments (double slashes)", kind, ErrInvalidArgument)
		}
	}
	return parts, nil
}

// ParseCollectionName parses a collection name string. It validates
// that the string is non-empty, contains no leading/trailing/double
// slashes, has an odd number of segments (collection paths alternate
// parent-collection / resource-id / child-collection), and that the
// trailing segment is a valid lowerCamelCase collection ID (starts
// with a lowercase letter) per AIP-122.
func ParseCollectionName(s string) (CollectionName, error) {
	parts, err := validateCanonicalPath("collection name", s)
	if err != nil {
		return "", err
	}
	if len(parts)%2 == 0 {
		return "", fmt.Errorf("collection name: %w: must have an odd number of segments (e.g. \"clusters\" or \"publishers/123/books\")", ErrInvalidArgument)
	}
	last := parts[len(parts)-1]
	if len(last) == 0 || last[0] < 'a' || last[0] > 'z' {
		return "", fmt.Errorf("collection name: %w: trailing segment must start with a lowercase letter (lowerCamelCase)", ErrInvalidArgument)
	}
	return CollectionName(s), nil
}

// CollectionID extracts the immediate (trailing) collection segment.
func (n CollectionName) CollectionID() CollectionID {
	parts := strings.Split(string(n), "/")
	return CollectionID(parts[len(parts)-1])
}

// Parent returns the parent resource name for a nested collection,
// or false for a flat (single-segment) collection.
func (n CollectionName) Parent() (ResourceName, bool) {
	s := string(n)
	idx := strings.LastIndex(s, "/")
	if idx < 0 {
		return "", false
	}
	return ResourceName(s[:idx]), true
}

// ResourceName is a collection-qualified, path-safe resource name
// (e.g. "clusters/prod"). Per AIP-122, this is the primary resource
// identifier — sometimes called "relative resource name" to
// distinguish from [FullResourceName].
type ResourceName string

// FullName composes a [FullResourceName] from this service name and
// the given resource name.
func (s ServiceName) FullName(name ResourceName) FullResourceName {
	return NewFullResourceName(s, name)
}

// FullResourceName is the globally unique name of the form
// "//{service}/{relative_name}" (e.g. "//kind.fleetshift.io/clusters/prod").
type FullResourceName string

// AliasNamespace scopes an alias key-space (e.g. "gcp", "aws").
type AliasNamespace string

// AliasKey is the key within an alias namespace (e.g. "project_id").
type AliasKey string

// AliasValue is the value of an alias (e.g. "my-project-123").
type AliasValue string

// RelationshipType classifies the relationship between two platform
// resources (e.g. "runs-on", "member-of").
type RelationshipType string

// NewRelationshipType validates and returns a [RelationshipType]. It
// rejects empty values.
func NewRelationshipType(s string) (RelationshipType, error) {
	if s == "" {
		return "", fmt.Errorf("relationship type: %w: must not be empty", ErrInvalidArgument)
	}
	return RelationshipType(s), nil
}

// ---------------------------------------------------------------------------
// Structured value types
// ---------------------------------------------------------------------------

// Alias is a cross-reference from an external naming scheme to a
// platform resource (e.g. GCP project ID -> platform UID).
//
// Construct with [NewAlias] to enforce invariants.
type Alias struct {
	Namespace AliasNamespace
	Key       AliasKey
	Value     AliasValue
}

// NewAlias validates and returns an [Alias]. All three fields must be
// non-empty.
func NewAlias(ns AliasNamespace, key AliasKey, value AliasValue) (Alias, error) {
	if ns == "" {
		return Alias{}, fmt.Errorf("alias namespace: %w: must not be empty", ErrInvalidArgument)
	}
	if key == "" {
		return Alias{}, fmt.Errorf("alias key: %w: must not be empty", ErrInvalidArgument)
	}
	if value == "" {
		return Alias{}, fmt.Errorf("alias value: %w: must not be empty", ErrInvalidArgument)
	}
	return Alias{Namespace: ns, Key: key, Value: value}, nil
}

// NewResourceName constructs a [ResourceName] from a collection and
// resource ID.
func NewResourceName(collection CollectionName, id ResourceID) (ResourceName, error) {
	return ResourceName(string(collection) + "/" + string(id)), nil
}

// ParseResourceName parses a resource name string into its typed form.
// It validates that the string contains no leading/trailing/double
// slashes and has an even number of segments (resource names alternate
// collection / resource-id, e.g. "clusters/prod" or
// "publishers/123/books/les-mis").
func ParseResourceName(s string) (ResourceName, error) {
	parts, err := validateCanonicalPath("resource name", s)
	if err != nil {
		return "", err
	}
	if len(parts)%2 != 0 {
		return "", fmt.Errorf("resource name: %w: must have an even number of segments (e.g. \"clusters/prod\")", ErrInvalidArgument)
	}
	return ResourceName(s), nil
}

// FullName composes a [FullResourceName] from the given service name
// and this resource name.
func (n ResourceName) FullName(service ServiceName) FullResourceName {
	return NewFullResourceName(service, n)
}

// Collection returns the full collection path (everything before the
// final ID segment).
func (n ResourceName) Collection() CollectionName {
	s := string(n)
	idx := strings.LastIndex(s, "/")
	if idx < 0 {
		return ""
	}
	return CollectionName(s[:idx])
}

// CollectionID extracts the immediate collection segment from the name.
func (n ResourceName) CollectionID() CollectionID {
	return n.Collection().CollectionID()
}

// ID extracts the resource ID segment (the final path component).
func (n ResourceName) ID() ResourceID {
	s := string(n)
	idx := strings.LastIndex(s, "/")
	if idx < 0 {
		return ResourceID(s)
	}
	return ResourceID(s[idx+1:])
}

// NewFullResourceName constructs a [FullResourceName] from a service
// name and resource name: "//{service}/{name}".
func NewFullResourceName(service ServiceName, name ResourceName) FullResourceName {
	return FullResourceName("//" + string(service) + "/" + string(name))
}

// ServiceName extracts the service segment from a full resource name.
func (n FullResourceName) ServiceName() ServiceName {
	s := strings.TrimPrefix(string(n), "//")
	parts := strings.SplitN(s, "/", 2)
	if len(parts) < 1 {
		return ""
	}
	return ServiceName(parts[0])
}

// ResourceName extracts the resource name segment from a full
// resource name.
func (n FullResourceName) ResourceName() ResourceName {
	s := strings.TrimPrefix(string(n), "//")
	parts := strings.SplitN(s, "/", 2)
	if len(parts) < 2 {
		return ""
	}
	return ResourceName(parts[1])
}

// ---------------------------------------------------------------------------
// PlatformResource aggregate
// ---------------------------------------------------------------------------

// PlatformResource is the canonical identity for a real-world resource
// in the fleet. It aggregates representations from multiple extension
// services, aliases, and relationships.
//
// Representations are not owned/mutated by this aggregate: they are
// derived on read by the repository (joining extension resources on
// name) and only ever populated here via [PlatformResourceFromSnapshot]
// for display. Aliases and relationships remain aggregate-owned.
//
// Construct new instances with [NewPlatformResource]; reconstitute from
// persistence with [PlatformResourceFromSnapshot]. Mutate via domain
// methods ([PlatformResource.SetLabels], [PlatformResource.AddAlias],
// etc.). Read via accessor methods.
type PlatformResource struct {
	name      ResourceName
	labels    map[string]string
	createdAt time.Time
	updatedAt time.Time

	representations []ResourceRepresentation
	aliases         []Alias
	relationships   []ResourceRelationship
}

// NewPlatformResource creates a brand-new [PlatformResource]. Use this
// on creation paths; use [PlatformResourceFromSnapshot] only for
// reconstituting from persistence.
//
// A platform resource has no UID of its own -- per AIP-148, a UID is
// only warranted when a resource can be deleted and recreated under
// the same name yet needs to be distinguished across that gap.
// Platform resources have no such generational concept: [ResourceName]
// is the sole, permanent identifier.
func NewPlatformResource(name ResourceName, labels map[string]string, now time.Time) *PlatformResource {
	if labels == nil {
		labels = map[string]string{}
	}
	return &PlatformResource{
		name:      name,
		labels:    labels,
		createdAt: now,
		updatedAt: now,
	}
}

// Collection returns the collection this resource belongs to,
// derived from its [ResourceName].
func (r *PlatformResource) Collection() CollectionName { return r.name.Collection() }

// Name returns the collection-qualified resource name.
func (r *PlatformResource) Name() ResourceName { return r.name }

// Labels returns the user-defined platform labels.
func (r *PlatformResource) Labels() map[string]string { return r.labels }

// CreatedAt returns the creation timestamp.
func (r *PlatformResource) CreatedAt() time.Time { return r.createdAt }

// UpdatedAt returns the last-updated timestamp.
func (r *PlatformResource) UpdatedAt() time.Time { return r.updatedAt }

// SetLabels replaces the platform labels and bumps updatedAt.
func (r *PlatformResource) SetLabels(labels map[string]string, now time.Time) {
	if labels == nil {
		labels = map[string]string{}
	}
	r.labels = labels
	r.updatedAt = now
}

// ---------------------------------------------------------------------------
// Child entity accessors
// ---------------------------------------------------------------------------

// Representations returns this platform resource's derived
// representations, as populated by the repository at load time (see
// [ResourceIdentityRepository.GetRepresentation]). Empty unless the
// aggregate was hydrated by a repository read that populates them.
func (r *PlatformResource) Representations() []ResourceRepresentation {
	return r.representations
}

// Aliases returns the aliases attached to this platform resource.
func (r *PlatformResource) Aliases() []Alias {
	return r.aliases
}

// Relationships returns the outgoing relationships from this platform
// resource.
func (r *PlatformResource) Relationships() []ResourceRelationship {
	return r.relationships
}

// ---------------------------------------------------------------------------
// Aggregate mutation methods
// ---------------------------------------------------------------------------

// AddAlias appends an alias to the platform resource. Duplicate aliases
// (same namespace+key+value) are silently ignored (idempotent). An alias
// whose namespace+key matches an existing alias but with a different
// value is rejected as an invariant violation. Cross-resource alias
// uniqueness is enforced by the repository on save.
func (r *PlatformResource) AddAlias(alias Alias) error {
	for _, existing := range r.aliases {
		if existing.Namespace == alias.Namespace && existing.Key == alias.Key {
			if existing.Value == alias.Value {
				return nil // idempotent
			}
			return fmt.Errorf("alias %s/%s already has value %q, cannot set %q: %w",
				existing.Namespace, existing.Key, existing.Value, alias.Value, ErrInvalidArgument)
		}
	}
	r.aliases = append(r.aliases, alias)
	return nil
}

// AddRelationship adds a typed relationship from this platform resource
// to another. Validates that the relationship type is non-empty and
// that the source name matches this aggregate. If a relationship with
// the same (type, targetName) already exists, it is updated in place.
func (r *PlatformResource) AddRelationship(rel ResourceRelationship) error {
	if rel.sourceName != r.name {
		return fmt.Errorf("relationship source name %q does not match resource name %q: %w",
			rel.sourceName, r.name, ErrInvalidArgument)
	}
	if rel.relType == "" {
		return fmt.Errorf("relationship type: %w: must not be empty", ErrInvalidArgument)
	}

	for i, existing := range r.relationships {
		if existing.relType == rel.relType && existing.targetName == rel.targetName {
			r.relationships[i] = rel
			return nil
		}
	}
	r.relationships = append(r.relationships, rel)
	return nil
}

// EffectiveLabels computes the merged label set. For now this returns
// the platform labels directly; in the future it will merge labels
// from linked extension resources via their UIDs.
func (r *PlatformResource) EffectiveLabels() map[string]string {
	result := make(map[string]string, len(r.labels))
	for k, v := range r.labels {
		result[k] = v
	}
	return result
}

// Snapshot returns a [PlatformResourceSnapshot] capturing all persisted
// state including child entities.
func (r *PlatformResource) Snapshot() PlatformResourceSnapshot {
	repSnaps := make([]ResourceRepresentationSnapshot, len(r.representations))
	for i, rep := range r.representations {
		repSnaps[i] = rep.Snapshot()
	}

	aliasSnaps := make([]ResourceAliasSnapshot, len(r.aliases))
	for i, a := range r.aliases {
		aliasSnaps[i] = ResourceAliasSnapshot{
			Namespace: a.Namespace,
			Key:       a.Key,
			Value:     a.Value,
		}
	}

	relSnaps := make([]ResourceRelationshipSnapshot, len(r.relationships))
	for i, rel := range r.relationships {
		relSnaps[i] = ResourceRelationshipSnapshot{
			SourceName:    rel.sourceName,
			Type:          rel.relType,
			TargetName:    rel.targetName,
			SourceService: rel.sourceService,
			CreatedAt:     rel.createdAt,
		}
	}

	return PlatformResourceSnapshot{
		Name:            r.name,
		Labels:          r.labels,
		CreatedAt:       r.createdAt,
		UpdatedAt:       r.updatedAt,
		Representations: repSnaps,
		Aliases:         aliasSnaps,
		Relationships:   relSnaps,
	}
}

// ---------------------------------------------------------------------------
// ResourceRepresentation -- an extension-service view of a platform resource
// ---------------------------------------------------------------------------

// ResourceRepresentation records that a specific extension service
// considers a platform resource to exist within its API surface. A
// single platform resource may have multiple representations (e.g. one
// from Kind, one from GCP Host Connector).
//
// Representations are never persisted directly: the repository derives
// them on read by joining extension resources to platform resources on
// name, so a representation appears and disappears exactly when its
// backing extension resource is created/deleted.
type ResourceRepresentation struct {
	serviceName          ServiceName
	version              APIVersion
	name                 ResourceName
	extensionResourceUID ExtensionResourceUID
	createdAt            time.Time
	updatedAt            time.Time
}

// FullResourceName returns the full resource name for this
// representation: "//{service}/{name}".
func (rr ResourceRepresentation) FullResourceName() FullResourceName {
	return rr.name.FullName(rr.serviceName)
}

// ServiceName returns the extension service that owns this representation.
func (rr ResourceRepresentation) ServiceName() ServiceName { return rr.serviceName }

// Version returns the API version of the representation.
func (rr ResourceRepresentation) Version() APIVersion { return rr.version }

// Name returns the identity-equivalent resource name.
func (rr ResourceRepresentation) Name() ResourceName { return rr.name }

// ExtensionResourceUID returns the linked extension resource UID.
func (rr ResourceRepresentation) ExtensionResourceUID() ExtensionResourceUID {
	return rr.extensionResourceUID
}

// CreatedAt returns the creation timestamp.
func (rr ResourceRepresentation) CreatedAt() time.Time { return rr.createdAt }

// UpdatedAt returns the last-updated timestamp.
func (rr ResourceRepresentation) UpdatedAt() time.Time { return rr.updatedAt }

// ResourceRepresentationFromSnapshot constructs a
// [ResourceRepresentation] from a snapshot.
func ResourceRepresentationFromSnapshot(s ResourceRepresentationSnapshot) ResourceRepresentation {
	return ResourceRepresentation{
		serviceName:          s.ServiceName,
		version:              s.Version,
		name:                 s.Name,
		extensionResourceUID: s.ExtensionResourceUID,
		createdAt:            s.CreatedAt,
		updatedAt:            s.UpdatedAt,
	}
}

// Snapshot returns a [ResourceRepresentationSnapshot].
func (rr ResourceRepresentation) Snapshot() ResourceRepresentationSnapshot {
	return ResourceRepresentationSnapshot{
		ServiceName:          rr.serviceName,
		Version:              rr.version,
		Name:                 rr.name,
		ExtensionResourceUID: rr.extensionResourceUID,
		CreatedAt:            rr.createdAt,
		UpdatedAt:            rr.updatedAt,
	}
}

// ---------------------------------------------------------------------------
// ResourceRelationship -- a typed edge between two platform resources
// ---------------------------------------------------------------------------

// ResourceRelationship records a directed relationship from one
// platform resource to another, reported by a particular extension
// service. Resources are referenced by [ResourceName] -- stable,
// human-readable, and the canonical AIP reference mechanism -- rather
// than by UID, since platform resources have none.
type ResourceRelationship struct {
	sourceName    ResourceName
	relType       RelationshipType
	targetName    ResourceName
	sourceService ServiceName
	createdAt     time.Time
}

// NewResourceRelationship constructs a [ResourceRelationship] entity.
// Aggregate-level invariants are enforced by
// [PlatformResource.AddRelationship].
func NewResourceRelationship(
	sourceName ResourceName,
	relType RelationshipType,
	targetName ResourceName,
	sourceService ServiceName,
	createdAt time.Time,
) ResourceRelationship {
	return ResourceRelationship{
		sourceName:    sourceName,
		relType:       relType,
		targetName:    targetName,
		sourceService: sourceService,
		createdAt:     createdAt,
	}
}

// SourceName returns the source platform resource name.
func (rr ResourceRelationship) SourceName() ResourceName { return rr.sourceName }

// Type returns the relationship type.
func (rr ResourceRelationship) Type() RelationshipType { return rr.relType }

// TargetName returns the target platform resource name.
func (rr ResourceRelationship) TargetName() ResourceName { return rr.targetName }

// SourceService returns the extension service that reported the relationship.
func (rr ResourceRelationship) SourceService() ServiceName { return rr.sourceService }

// CreatedAt returns the creation timestamp.
func (rr ResourceRelationship) CreatedAt() time.Time { return rr.createdAt }

// ResourceRelationshipFromSnapshot constructs a [ResourceRelationship]
// from a snapshot.
func ResourceRelationshipFromSnapshot(s ResourceRelationshipSnapshot) ResourceRelationship {
	return ResourceRelationship{
		sourceName:    s.SourceName,
		relType:       s.Type,
		targetName:    s.TargetName,
		sourceService: s.SourceService,
		createdAt:     s.CreatedAt,
	}
}
