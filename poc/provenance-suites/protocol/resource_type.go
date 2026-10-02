package protocol

import (
	"fmt"
	"strings"
	"unicode"
)

// ValidateResourceType checks the full, versionless service/type identity used
// by managed-resource authorizations and fulfillment relations. Both components
// must be nonempty, with one slash and no whitespace. Callers compare complete
// values exactly; validation does not normalize them or impose an addon naming
// policy.
func ValidateResourceType(resourceType string) error {
	service, kind, ok := strings.Cut(resourceType, "/")
	if !ok || service == "" || kind == "" || strings.Contains(kind, "/") || strings.ContainsFunc(resourceType, unicode.IsSpace) {
		return fmt.Errorf("%w: resource type %q must be service/type with no whitespace", ErrMalformedEvidence, resourceType)
	}
	return nil
}
