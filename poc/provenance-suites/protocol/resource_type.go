package protocol

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
)

// ResourceType is an immutable full, versionless service/type identity used by
// managed-resource authorizations and fulfillment relations. Construct it with
// [ParseResourceType]. Its zero value is unconstructed and cannot be serialized.
// A parsed value establishes syntax; it grants no authenticity or authority.
type ResourceType struct{ value string }

// ParseResourceType constructs a ResourceType with two nonempty components,
// one slash, and no whitespace. It preserves the complete identity exactly;
// it does not normalize values or impose an addon naming policy.
func ParseResourceType(value string) (ResourceType, error) {
	service, kind, ok := strings.Cut(value, "/")
	if !ok || service == "" || kind == "" || strings.Contains(kind, "/") || strings.ContainsFunc(value, unicode.IsSpace) {
		return ResourceType{}, fmt.Errorf("%w: resource type %q must be service/type with no whitespace", ErrMalformedEvidence, value)
	}
	return ResourceType{value: value}, nil
}

// String returns the complete resource type as supplied to its parser.
func (r ResourceType) String() string { return r.value }

// MarshalJSON preserves the predicate's resource_type JSON string representation
// and rejects an unconstructed value at the assertion encoding boundary.
func (r ResourceType) MarshalJSON() ([]byte, error) {
	if r == (ResourceType{}) {
		return nil, fmt.Errorf("%w: resource type is required", ErrMalformedEvidence)
	}
	return json.Marshal(r.value)
}

// UnmarshalJSON constructs a ResourceType from a JSON string. A failed parse
// leaves the receiver unchanged. Predicate decoders also reject omitted fields.
func (r *ResourceType) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("%w: decode resource type: %v", ErrMalformedEvidence, err)
	}
	parsed, err := ParseResourceType(value)
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}
