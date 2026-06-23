package domain

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ResourceType identifies a kind of managed resource as registered by
// an addon (e.g. "kind.fleetshift.io/Cluster"). Per AIP-123, resource
// types follow the pattern {ServiceName}/{Type} where ServiceName is
// the AIP-122 service name and Type is the PascalCase singular proto
// message name.
//
// ResourceType is used for routing, schema lookup, and fulfillment
// relation resolution — not as a resource identity key. See
// [ManifestType] for the decoupled manifest dispatch label.
type ResourceType string

// NewResourceType constructs a [ResourceType] from a [ServiceName] and
// a PascalCase type name per AIP-123. The type name must start with an
// uppercase letter and contain only alphanumeric characters.
func NewResourceType(service ServiceName, typeName string) (ResourceType, error) {
	if err := validateTypeName(typeName); err != nil {
		return "", err
	}
	return ResourceType(string(service) + "/" + typeName), nil
}

// ParseResourceType validates and returns a [ResourceType] from a raw
// string in the AIP-123 format "{ServiceName}/{Type}".
func ParseResourceType(s string) (ResourceType, error) {
	if s == "" {
		return "", fmt.Errorf("resource type: %w: must not be empty", ErrInvalidArgument)
	}
	parts := strings.SplitN(s, "/", 3)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("resource type: %w: must be {ServiceName}/{Type}", ErrInvalidArgument)
	}
	if err := validateTypeName(parts[1]); err != nil {
		return "", err
	}
	return ResourceType(s), nil
}

// ServiceName extracts the service component from a resource type.
// Returns empty for malformed values.
func (rt ResourceType) ServiceName() ServiceName {
	if i := strings.IndexByte(string(rt), '/'); i > 0 {
		return ServiceName(rt[:i])
	}
	return ""
}

// TypeName extracts the type component from a resource type.
// Returns empty for malformed values.
func (rt ResourceType) TypeName() string {
	if i := strings.IndexByte(string(rt), '/'); i >= 0 && i < len(rt)-1 {
		return string(rt[i+1:])
	}
	return ""
}

func validateTypeName(s string) error {
	if s == "" {
		return fmt.Errorf("resource type: %w: type name must not be empty", ErrInvalidArgument)
	}
	if s[0] < 'A' || s[0] > 'Z' {
		return fmt.Errorf("resource type: %w: type name must start with uppercase letter", ErrInvalidArgument)
	}
	for _, c := range s {
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
			return fmt.Errorf("resource type: %w: type name must be alphanumeric PascalCase", ErrInvalidArgument)
		}
	}
	return nil
}

// ManagedResourceUID is the opaque, stable identifier for a managed
// resource instance. Generated once at creation time and never
// changes. The underlying type is [uuid.UUID] so structural validity
// is encoded in the type system.
type ManagedResourceUID uuid.UUID

// NewManagedResourceUID generates a new random [ManagedResourceUID].
func NewManagedResourceUID() ManagedResourceUID {
	return ManagedResourceUID(uuid.New())
}

// ParseManagedResourceUID parses a string into a [ManagedResourceUID].
func ParseManagedResourceUID(s string) (ManagedResourceUID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return ManagedResourceUID{}, fmt.Errorf("managed resource uid: %w", err)
	}
	return ManagedResourceUID(u), nil
}

// String returns the canonical UUID string representation.
func (u ManagedResourceUID) String() string { return uuid.UUID(u).String() }

// MarshalText implements [encoding.TextMarshaler] for JSON string encoding.
func (u ManagedResourceUID) MarshalText() ([]byte, error) { return uuid.UUID(u).MarshalText() }

// UnmarshalText implements [encoding.TextUnmarshaler] for JSON string decoding.
func (u *ManagedResourceUID) UnmarshalText(data []byte) error {
	return (*uuid.UUID)(u).UnmarshalText(data)
}

// Value implements [driver.Valuer] for SQL persistence.
func (u ManagedResourceUID) Value() (driver.Value, error) { return uuid.UUID(u).String(), nil }

// Scan implements [sql.Scanner] for SQL hydration.
func (u *ManagedResourceUID) Scan(src any) error { return (*uuid.UUID)(u).Scan(src) }

// IsZero returns true when the UID is the zero (nil) UUID.
func (u ManagedResourceUID) IsZero() bool { return uuid.UUID(u) == uuid.Nil }

// IntentVersion is a monotonically increasing counter for versioned
// resource intent within a managed resource. Each spec update creates
// a new version; the HEAD table tracks which version is current.
type IntentVersion int64

// ManagedResourceTypeDef is the metadata record that an addon registers
// to declare ownership of a managed resource type. It carries the
// fulfillment relation (how resources of this type map to fulfillments)
// and the addon's cryptographic proof of that claim.
//
// Spec validation is handled at the transport layer by protovalidate
// using buf.validate annotations from the addon's spec proto. No schema
// is stored in the type definition.
type ManagedResourceTypeDef struct {
	ResourceType   ResourceType
	Relation       FulfillmentRelation
	Signature      Signature
	APIServiceName ServiceName
	APIVersion     APIVersion
	// TODO: Note that this is currently limited to a single, non-nested collection.
	// It will have to be a separate feature to expand type defs to introduce a configurable parent collection.
	CollectionID CollectionID
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// MarshalJSON implements json.Marshaler so the interface-typed Relation
// field survives JSON round-trips (required for durable workflow replay).
func (d ManagedResourceTypeDef) MarshalJSON() ([]byte, error) {
	rel, err := marshalFulfillmentRelation(d.Relation)
	if err != nil {
		return nil, err
	}
	type alias struct {
		ResourceType   ResourceType       `json:"ResourceType"`
		Relation       fulfillmentRelJSON `json:"Relation"`
		Signature      Signature          `json:"Signature"`
		APIServiceName ServiceName        `json:"APIServiceName,omitempty"`
		APIVersion     APIVersion         `json:"APIVersion,omitempty"`
		CollectionID   CollectionID       `json:"CollectionID,omitempty"`
		CreatedAt      time.Time          `json:"CreatedAt"`
		UpdatedAt      time.Time          `json:"UpdatedAt"`
	}
	return json.Marshal(alias{
		ResourceType:   d.ResourceType,
		Relation:       rel,
		Signature:      d.Signature,
		APIServiceName: d.APIServiceName,
		APIVersion:     d.APIVersion,
		CollectionID:   d.CollectionID,
		CreatedAt:      d.CreatedAt,
		UpdatedAt:      d.UpdatedAt,
	})
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *ManagedResourceTypeDef) UnmarshalJSON(data []byte) error {
	type alias struct {
		ResourceType   ResourceType       `json:"ResourceType"`
		Relation       fulfillmentRelJSON `json:"Relation"`
		Signature      Signature          `json:"Signature"`
		APIServiceName ServiceName        `json:"APIServiceName,omitempty"`
		APIVersion     APIVersion         `json:"APIVersion,omitempty"`
		CollectionID   CollectionID       `json:"CollectionID,omitempty"`
		CreatedAt      time.Time          `json:"CreatedAt"`
		UpdatedAt      time.Time          `json:"UpdatedAt"`
	}
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	d.ResourceType = a.ResourceType
	d.Signature = a.Signature
	d.APIServiceName = a.APIServiceName
	d.APIVersion = a.APIVersion
	d.CollectionID = a.CollectionID
	d.CreatedAt = a.CreatedAt
	d.UpdatedAt = a.UpdatedAt
	rel, err := unmarshalFulfillmentRelation(a.Relation)
	if err != nil {
		return err
	}
	d.Relation = rel
	return nil
}

// ResourceIntent is an immutable version of a managed resource spec.
// INSERT only — never updated. The managed resource HEAD table tracks
// which version is current.
type ResourceIntent struct {
	ResourceType ResourceType
	Name         ResourceName
	Version      IntentVersion
	Spec         json.RawMessage
	CreatedAt    time.Time
}

// ManagedResource is the HEAD table aggregate for a managed resource
// instance. It owns identity, fulfillment linkage, and intent
// versioning. Mutations that affect orchestration go through the
// referenced [Fulfillment].
//
// Construct new instances with [NewManagedResource]; reconstitute from
// persistence with [ManagedResourceFromSnapshot]. Intent recording goes
// through [ManagedResource.RecordIntent].
type ManagedResource struct {
	resourceType   ResourceType
	name           ResourceName
	uid            ManagedResourceUID
	currentVersion IntentVersion
	fulfillmentID  FulfillmentID
	createdAt      time.Time
	updatedAt      time.Time
	deletedAt      *time.Time

	pendingIntents []ResourceIntent
}

// NewManagedResource creates a brand-new [ManagedResource]. Use this
// on creation paths; use [ManagedResourceFromSnapshot] only for
// reconstituting from persistence.
//
// After construction, call [ManagedResource.RecordIntent] to attach
// the initial spec version.
func NewManagedResource(resourceType ResourceType, name ResourceName, uid ManagedResourceUID, fulfillmentID FulfillmentID, now time.Time) *ManagedResource {
	return &ManagedResource{
		resourceType:  resourceType,
		name:          name,
		uid:           uid,
		fulfillmentID: fulfillmentID,
		createdAt:     now,
		updatedAt:     now,
	}
}

// RecordIntent advances the intent version and collects a pending
// [ResourceIntent] record for the repository to flush. Returns the
// recorded intent for use in downstream derivation (e.g.
// [FulfillmentRelation.DeriveStrategies]). This is the only way to
// create intents — the aggregate owns the version counter.
func (mr *ManagedResource) RecordIntent(spec json.RawMessage, now time.Time) ResourceIntent {
	mr.currentVersion++
	intent := ResourceIntent{
		ResourceType: mr.resourceType,
		Name:         mr.name,
		Version:      mr.currentVersion,
		Spec:         spec,
		CreatedAt:    now,
	}
	mr.pendingIntents = append(mr.pendingIntents, intent)
	return intent
}

// DrainPendingIntents returns the pending intents collected by
// [ManagedResource.RecordIntent] and nils the internal buffer.
// Repositories call this to extract intents for flushing to storage,
// ensuring each intent is written exactly once. Subsequent calls (or
// [ManagedResource.Snapshot]) will see an empty pending buffer.
func (mr *ManagedResource) DrainPendingIntents() []ResourceIntent {
	intents := mr.pendingIntents
	mr.pendingIntents = nil
	return intents
}

// Accessor methods -- read-only getters for private fields.

// ResourceType returns the managed resource type.
func (mr *ManagedResource) ResourceType() ResourceType { return mr.resourceType }

// Name returns the resource instance name.
func (mr *ManagedResource) Name() ResourceName { return mr.name }

// UID returns the resource's external UID.
func (mr *ManagedResource) UID() ManagedResourceUID { return mr.uid }

// CurrentVersion returns the current intent version.
func (mr *ManagedResource) CurrentVersion() IntentVersion { return mr.currentVersion }

// FulfillmentID returns the linked fulfillment's identifier.
func (mr *ManagedResource) FulfillmentID() FulfillmentID { return mr.fulfillmentID }

// CreatedAt returns the creation timestamp.
func (mr *ManagedResource) CreatedAt() time.Time { return mr.createdAt }

// UpdatedAt returns the last-updated timestamp.
func (mr *ManagedResource) UpdatedAt() time.Time { return mr.updatedAt }

// DeletedAt returns the deletion timestamp, if soft-deleted.
func (mr *ManagedResource) DeletedAt() *time.Time { return mr.deletedAt }

// ManagedResourceView is the read model that joins a [ManagedResource]
// with its current [ResourceIntent] and [Fulfillment]. Constructed by
// the repository via joins; never written directly.
type ManagedResourceView struct {
	ManagedResource ManagedResource
	Intent          ResourceIntent
	Fulfillment     Fulfillment
}
