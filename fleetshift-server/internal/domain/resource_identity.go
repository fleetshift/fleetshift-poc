package domain

import (
	"fmt"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Value types for platform resource identity
//
// These are distinct from the existing ResourceName (which means the leaf
// managed-resource name, not the AIP relative name). Do not reuse
// domain.ResourceName for platform identity.
// ---------------------------------------------------------------------------

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
// empty values, non-lower-case values, and values containing '/'.
func NewCollectionID(s string) (CollectionID, error) {
	if s == "" {
		return "", fmt.Errorf("collection id: %w: must not be empty", ErrInvalidArgument)
	}
	if s != strings.ToLower(s) {
		return "", fmt.Errorf("collection id: %w: must be lower-case", ErrInvalidArgument)
	}
	if strings.Contains(s, "/") {
		return "", fmt.Errorf("collection id: %w: must not contain '/'", ErrInvalidArgument)
	}
	return CollectionID(s), nil
}

// RelativeResourceName is a collection-qualified, path-safe resource name
// (e.g. "clusters/prod"). It takes the form "{collection}/{id}".
type RelativeResourceName string

// FullResourceName is the globally unique name of the form
// "//{service}/{relative_name}" (e.g. "//kind.fleetshift.io/clusters/prod").
type FullResourceName string

// PlatformResourceUID is the opaque, stable identifier for a platform
// resource. Generated once at claim time and never changes.
type PlatformResourceUID string

// AliasNamespace scopes an alias key-space (e.g. "gcp", "aws").
type AliasNamespace string

// AliasKey is the key within an alias namespace (e.g. "project_id").
type AliasKey string

// AliasValue is the value of an alias (e.g. "my-project-123").
type AliasValue string

// RepresentationRole classifies what a representation means relative to
// the platform resource.
type RepresentationRole string

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

const (
	// RepresentationRoleManaged marks a representation as managed by the
	// platform (e.g. a managed Kind cluster).
	RepresentationRoleManaged RepresentationRole = "managed"

	// RepresentationRoleInventory marks a representation as discovered
	// by an inventory provider.
	RepresentationRoleInventory RepresentationRole = "inventory"

	// RepresentationRoleTarget marks a representation as a delivery
	// target.
	RepresentationRoleTarget RepresentationRole = "target"
)

// knownRoles is the set of valid representation roles.
var knownRoles = map[RepresentationRole]bool{
	RepresentationRoleManaged:   true,
	RepresentationRoleInventory: true,
	RepresentationRoleTarget:    true,
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

// NewRelativeResourceName constructs a [RelativeResourceName] from a
// collection and resource ID. It validates the id segment; the
// collection is assumed valid because it is already a [CollectionID].
func NewRelativeResourceName(collection CollectionID, id string) (RelativeResourceName, error) {
	if collection == "" {
		return "", fmt.Errorf("relative resource name: %w: collection must not be empty", ErrInvalidArgument)
	}
	if id == "" {
		return "", fmt.Errorf("relative resource name: %w: id must not be empty", ErrInvalidArgument)
	}
	if strings.Contains(id, "/") {
		return "", fmt.Errorf("relative resource name: %w: id must not contain '/'", ErrInvalidArgument)
	}
	return RelativeResourceName(string(collection) + "/" + id), nil
}

// CollectionID extracts the collection segment from a relative name.
func (n RelativeResourceName) CollectionID() CollectionID {
	parts := strings.SplitN(string(n), "/", 2)
	if len(parts) < 1 {
		return ""
	}
	return CollectionID(parts[0])
}

// ID extracts the resource ID segment from a relative name.
func (n RelativeResourceName) ID() string {
	parts := strings.SplitN(string(n), "/", 2)
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}

// NewFullResourceName constructs a [FullResourceName] from a service
// name and relative name: "//{service}/{relative_name}".
func NewFullResourceName(service ServiceName, name RelativeResourceName) FullResourceName {
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

// RelativeName extracts the relative resource name segment from a full
// resource name.
func (n FullResourceName) RelativeName() RelativeResourceName {
	s := strings.TrimPrefix(string(n), "//")
	parts := strings.SplitN(s, "/", 2)
	if len(parts) < 2 {
		return ""
	}
	return RelativeResourceName(parts[1])
}

// validateRepresentationRoles checks that roles is non-empty, all
// values are known, and "managed" and "inventory" are not combined.
// This is an aggregate-internal invariant check used by
// [PlatformResource.AttachRepresentation].
func validateRepresentationRoles(roles []RepresentationRole) error {
	if len(roles) == 0 {
		return fmt.Errorf("representation roles: %w: at least one role required", ErrInvalidArgument)
	}
	hasManaged := false
	hasInventory := false
	for _, r := range roles {
		if !knownRoles[r] {
			return fmt.Errorf("representation role %q: %w: unknown role", r, ErrInvalidArgument)
		}
		if r == RepresentationRoleManaged {
			hasManaged = true
		}
		if r == RepresentationRoleInventory {
			hasInventory = true
		}
	}
	if hasManaged && hasInventory {
		return fmt.Errorf("representation roles: %w: managed and inventory must not be combined", ErrInvalidArgument)
	}
	return nil
}

// ---------------------------------------------------------------------------
// PlatformResource aggregate
// ---------------------------------------------------------------------------

// PlatformResource is the canonical identity for a real-world resource
// in the fleet. It aggregates representations from multiple extension
// services, aliases, and relationships.
//
// Construct new instances with [NewPlatformResource]; reconstitute from
// persistence with [PlatformResourceFromSnapshot]. Mutate via domain
// methods ([PlatformResource.SetLabels], [PlatformResource.AttachRepresentation],
// etc.). Read via accessor methods.
type PlatformResource struct {
	uid          PlatformResourceUID
	collectionID CollectionID
	relativeName RelativeResourceName
	labels       map[string]string
	createdAt    time.Time
	updatedAt    time.Time
	deletedAt    *time.Time

	representations []ResourceRepresentation
	aliases         []Alias
	relationships   []ResourceRelationship
}

// NewPlatformResource creates a brand-new [PlatformResource]. Use this
// on creation paths; use [PlatformResourceFromSnapshot] only for
// reconstituting from persistence.
func NewPlatformResource(uid PlatformResourceUID, collectionID CollectionID, relativeName RelativeResourceName, labels map[string]string, now time.Time) *PlatformResource {
	if labels == nil {
		labels = map[string]string{}
	}
	return &PlatformResource{
		uid:          uid,
		collectionID: collectionID,
		relativeName: relativeName,
		labels:       labels,
		createdAt:    now,
		updatedAt:    now,
	}
}

// UID returns the platform resource's stable unique identifier.
func (r *PlatformResource) UID() PlatformResourceUID { return r.uid }

// CollectionID returns the collection this resource belongs to.
func (r *PlatformResource) CollectionID() CollectionID { return r.collectionID }

// RelativeName returns the collection-qualified resource name.
func (r *PlatformResource) RelativeName() RelativeResourceName { return r.relativeName }

// Labels returns the user-defined platform labels.
func (r *PlatformResource) Labels() map[string]string { return r.labels }

// CreatedAt returns the creation timestamp.
func (r *PlatformResource) CreatedAt() time.Time { return r.createdAt }

// UpdatedAt returns the last-updated timestamp.
func (r *PlatformResource) UpdatedAt() time.Time { return r.updatedAt }

// DeletedAt returns the soft-delete timestamp, or nil if active.
func (r *PlatformResource) DeletedAt() *time.Time { return r.deletedAt }

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

// Representations returns the active (non-tombstoned) representations.
func (r *PlatformResource) Representations() []ResourceRepresentation {
	var active []ResourceRepresentation
	for _, rep := range r.representations {
		if rep.DeletedAt == nil {
			active = append(active, rep)
		}
	}
	return active
}

// AllRepresentations returns all representations including tombstoned
// ones.
func (r *PlatformResource) AllRepresentations() []ResourceRepresentation {
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

// AttachRepresentationInput is the input for
// [PlatformResource.AttachRepresentation].
//
// CollectionID and RelativeName are not included because the relative
// resource name is identity-equivalent across services (see
// resource_identity_and_api.md). The aggregate stamps them from its own
// canonical identity.
type AttachRepresentationInput struct {
	ServiceName ServiceName
	Version     APIVersion
	Roles       []RepresentationRole
	Labels      map[string]string
}

// AttachRepresentation adds or updates an extension representation on
// this platform resource. The representation inherits the aggregate's
// canonical CollectionID and RelativeName because the relative resource
// name is identity-equivalent across services. It validates that
// managed+inventory roles are not combined; other value-object
// invariants are assumed enforced at construction time by callers.
func (r *PlatformResource) AttachRepresentation(in AttachRepresentationInput, now time.Time) error {
	if err := validateRepresentationRoles(in.Roles); err != nil {
		return err
	}

	rep := ResourceRepresentation{
		PlatformUID:  r.uid,
		ServiceName:  in.ServiceName,
		Version:      in.Version,
		CollectionID: r.collectionID,
		RelativeName: r.relativeName,
		Roles:        in.Roles,
		Labels:       in.Labels,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	for i, existing := range r.representations {
		if existing.ServiceName == in.ServiceName {
			rep.CreatedAt = existing.CreatedAt
			rep.DeletedAt = nil
			r.representations[i] = rep
			r.updatedAt = now
			return nil
		}
	}

	r.representations = append(r.representations, rep)
	r.updatedAt = now
	return nil
}

// TombstoneRepresentation marks the representation from the given
// service as deleted. Since the relative resource name is identity-
// equivalent across services, the match is by ServiceName only.
// Returns [ErrNotFound] if no active representation matches.
func (r *PlatformResource) TombstoneRepresentation(service ServiceName, now time.Time) error {
	for i, rep := range r.representations {
		if rep.ServiceName == service && rep.DeletedAt == nil {
			r.representations[i].DeletedAt = &now
			r.representations[i].UpdatedAt = now
			r.updatedAt = now
			return nil
		}
	}
	return fmt.Errorf("representation from %s on %s: %w", service, r.relativeName, ErrNotFound)
}

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
// that the source UID matches this aggregate. If a relationship with
// the same (type, targetUID) already exists, it is updated in place.
func (r *PlatformResource) AddRelationship(rel ResourceRelationship) error {
	if rel.SourceUID != r.uid {
		return fmt.Errorf("relationship source UID %q does not match resource UID %q: %w",
			rel.SourceUID, r.uid, ErrInvalidArgument)
	}
	if rel.Type == "" {
		return fmt.Errorf("relationship type: %w: must not be empty", ErrInvalidArgument)
	}

	for i, existing := range r.relationships {
		if existing.Type == rel.Type && existing.TargetUID == rel.TargetUID {
			r.relationships[i] = rel
			return nil
		}
	}
	r.relationships = append(r.relationships, rel)
	return nil
}

// EffectiveLabels computes the merged label set. Platform labels remain
// unprefixed; active representation labels are prefixed with
// "{service_name}/{key}". Platform labels take priority in the event of
// a key collision with a prefixed representation label.
func (r *PlatformResource) EffectiveLabels() map[string]string {
	result := make(map[string]string)
	for _, rep := range r.representations {
		if rep.DeletedAt != nil {
			continue
		}
		prefix := string(rep.ServiceName) + "/"
		for k, v := range rep.Labels {
			result[prefix+k] = v
		}
	}
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
			SourceUID:     rel.SourceUID,
			Type:          rel.Type,
			TargetUID:     rel.TargetUID,
			SourceService: rel.SourceService,
			CreatedAt:     rel.CreatedAt,
		}
	}

	return PlatformResourceSnapshot{
		UID:             r.uid,
		CollectionID:    r.collectionID,
		RelativeName:    r.relativeName,
		Labels:          r.labels,
		CreatedAt:       r.createdAt,
		UpdatedAt:       r.updatedAt,
		DeletedAt:       r.deletedAt,
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
type ResourceRepresentation struct {
	PlatformUID  PlatformResourceUID
	ServiceName  ServiceName
	Version      APIVersion
	CollectionID CollectionID
	RelativeName RelativeResourceName
	Roles        []RepresentationRole
	Labels       map[string]string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	DeletedAt    *time.Time
}

// FullResourceName returns the full resource name for this
// representation: "//{service}/{relative_name}".
func (rr ResourceRepresentation) FullResourceName() FullResourceName {
	return NewFullResourceName(rr.ServiceName, rr.RelativeName)
}

// ResourceRepresentationFromSnapshot constructs a
// [ResourceRepresentation] from a snapshot.
func ResourceRepresentationFromSnapshot(s ResourceRepresentationSnapshot) ResourceRepresentation {
	return ResourceRepresentation{
		PlatformUID:  s.PlatformUID,
		ServiceName:  s.ServiceName,
		Version:      s.Version,
		CollectionID: s.CollectionID,
		RelativeName: s.RelativeName,
		Roles:        s.Roles,
		Labels:       s.Labels,
		CreatedAt:    s.CreatedAt,
		UpdatedAt:    s.UpdatedAt,
		DeletedAt:    s.DeletedAt,
	}
}

// Snapshot returns a [ResourceRepresentationSnapshot].
func (rr ResourceRepresentation) Snapshot() ResourceRepresentationSnapshot {
	return ResourceRepresentationSnapshot{
		PlatformUID:  rr.PlatformUID,
		ServiceName:  rr.ServiceName,
		Version:      rr.Version,
		CollectionID: rr.CollectionID,
		RelativeName: rr.RelativeName,
		Roles:        rr.Roles,
		Labels:       rr.Labels,
		CreatedAt:    rr.CreatedAt,
		UpdatedAt:    rr.UpdatedAt,
		DeletedAt:    rr.DeletedAt,
	}
}

// ---------------------------------------------------------------------------
// ResourceRelationship -- a typed edge between two platform resources
// ---------------------------------------------------------------------------

// ResourceRelationship records a directed relationship from one
// platform resource to another, reported by a particular extension
// service.
//
// TODO: Relationships currently reference resources by UID. Resource
// names (RelativeResourceName) are stable, human-readable, and the
// canonical AIP reference mechanism. UIDs force an extra lookup to
// understand what a relationship points to. Consider switching to
// names, possibly with deferred resolution for cases where the target
// resource doesn't exist yet.
type ResourceRelationship struct {
	SourceUID     PlatformResourceUID
	Type          RelationshipType
	TargetUID     PlatformResourceUID
	SourceService ServiceName
	CreatedAt     time.Time
}

// ResourceRelationshipFromSnapshot constructs a [ResourceRelationship]
// from a snapshot.
func ResourceRelationshipFromSnapshot(s ResourceRelationshipSnapshot) ResourceRelationship {
	return ResourceRelationship{
		SourceUID:     s.SourceUID,
		Type:          s.Type,
		TargetUID:     s.TargetUID,
		SourceService: s.SourceService,
		CreatedAt:     s.CreatedAt,
	}
}
