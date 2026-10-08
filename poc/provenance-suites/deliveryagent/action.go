package deliveryagent

import "github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"

// AuthorizedAction is the ordinary delivery value derived by common semantics
// after successful authentication, early admission, and all required premises.
// Its manifests and basis are immutable once shared within common code.
type AuthorizedAction struct {
	// RootEvidence is the immutable outer evidence identity of the intent.
	RootEvidence protocol.Digest
	// Principal is the successful root verifier's authenticated principal.
	Principal protocol.Principal
	// Scope is the exact authenticated tenant, target, resource, generation,
	// and action admitted by the common handler.
	Scope protocol.DeliveryScope
	// PredicateType is the authenticated root's purpose.
	PredicateType protocol.PredicateType
	// Manifests are the final media-typed payloads derived from authenticated content.
	Manifests []protocol.TypedManifest
	// Basis is the sorted unique union of selected evidence identities and
	// their successful normalized validity basis, including retained evidence.
	Basis []protocol.Digest
}

// authorizedAction materializes the common result after the handler's required
// semantics succeed. Scope and manifests are already derived immutable values;
// basis finalization adds no authorization decision or evidence traversal.
func (s *verificationSession) authorizedAction(root verifiedNode, scope protocol.DeliveryScope, manifests []protocol.TypedManifest) AuthorizedAction {
	return AuthorizedAction{
		RootEvidence:  root.identity,
		Principal:     root.result.Authenticated.Principal,
		Scope:         scope,
		PredicateType: root.result.Authenticated.PredicateType,
		Manifests:     manifests,
		Basis:         s.selectedBasis(),
	}
}
