package domain

import (
	"encoding/json"
	"time"
)

// ResourceName uniquely identifies a managed resource instance within
// its [ResourceType]. Following AIP naming conventions, this is the
// leaf segment of the resource name (e.g. "prod-us-east-1" for a
// resource named "clusters/prod-us-east-1").
type ResourceName string

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
	ResourceType ResourceType
	Relation     FulfillmentRelation
	Signature    Signature
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
		ResourceType ResourceType       `json:"ResourceType"`
		Relation     fulfillmentRelJSON `json:"Relation"`
		Signature    Signature          `json:"Signature"`
		CreatedAt    time.Time          `json:"CreatedAt"`
		UpdatedAt    time.Time          `json:"UpdatedAt"`
	}
	return json.Marshal(alias{
		ResourceType: d.ResourceType,
		Relation:     rel,
		Signature:    d.Signature,
		CreatedAt:    d.CreatedAt,
		UpdatedAt:    d.UpdatedAt,
	})
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *ManagedResourceTypeDef) UnmarshalJSON(data []byte) error {
	type alias struct {
		ResourceType ResourceType       `json:"ResourceType"`
		Relation     fulfillmentRelJSON `json:"Relation"`
		Signature    Signature          `json:"Signature"`
		CreatedAt    time.Time          `json:"CreatedAt"`
		UpdatedAt    time.Time          `json:"UpdatedAt"`
	}
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	d.ResourceType = a.ResourceType
	d.Signature = a.Signature
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
	uid            string
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
func NewManagedResource(resourceType ResourceType, name ResourceName, uid string, fulfillmentID FulfillmentID, now time.Time) *ManagedResource {
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
func (mr *ManagedResource) UID() string { return mr.uid }

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
