package protocol

import "fmt"

// IdentityScheme is a well-known, versioned identity scheme whose
// implementation owns canonical parsing and comparison.
type IdentityScheme string

const (
	// IdentitySchemeOIDCSubV1 identifies an exact OIDC issuer and subject.
	IdentitySchemeOIDCSubV1 IdentityScheme = "oidc-sub/v1"
)

// Authority is the canonical external namespace for subjects under a scheme.
type Authority string

// Subject is the scheme-canonical subject identifier.
type Subject string

// TenantPartition is an optional external tenant partition scoped to its own
// (scheme, authority). An empty value means the principal has no partition.
type TenantPartition string

// TenantID is a FleetShift tenant identifier produced by tenant mapping.
type TenantID string

// FullResourceName is an AIP-122 full resource name of the form
// "//{service}/{collection}/{id}", for example
// "//fleetshift.io/deployments/web" or
// "//kind.fleetshift.io/clusters/prod". It is the producer-defined unique
// identity of a Deployment or ManagedResource. That resource is 1-1 with
// its underlying fulfillment; producers do not know an RM-assigned
// fulfillment ID.
type FullResourceName string

// PrincipalAuthority is the (scheme, authority) key of an AuthorityConfig.
type PrincipalAuthority struct {
	Scheme    IdentityScheme `json:"scheme"`
	Authority Authority      `json:"authority"`
}

// Principal is the canonical identity yielded by successful verification.
type Principal struct {
	Scheme          IdentityScheme  `json:"scheme"`
	Authority       Authority       `json:"authority"`
	TenantPartition TenantPartition `json:"tenant_partition,omitempty"`
	Subject         Subject         `json:"subject"`
}

// Authority returns the principal's authority key.
func (p Principal) PrincipalAuthority() PrincipalAuthority {
	return PrincipalAuthority{Scheme: p.Scheme, Authority: p.Authority}
}

// Canonicalize returns the principal after scheme-owned parsing and
// comparison normalization. Common code does not apply generic URL or
// Unicode normalization.
func (p Principal) Canonicalize() (Principal, error) {
	switch p.Scheme {
	case IdentitySchemeOIDCSubV1:
		if p.Authority == "" || p.Subject == "" {
			return Principal{}, fmt.Errorf("oidc-sub/v1 authority and subject are required")
		}
		return p, nil
	case "":
		return Principal{}, fmt.Errorf("identity scheme is required")
	default:
		return Principal{}, fmt.Errorf("unknown identity scheme %q", p.Scheme)
	}
}

// Equal reports whether two principals identify the same canonical identity.
func (p Principal) Equal(other Principal) bool {
	return p == other
}
