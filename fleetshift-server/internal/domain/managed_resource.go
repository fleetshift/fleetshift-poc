package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
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

// IntentVersion is a monotonically increasing counter for versioned
// resource intent within a managed resource. Each spec update creates
// a new version; the HEAD table tracks which version is current.
type IntentVersion int64

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
