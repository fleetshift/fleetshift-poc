package deliveryagent

import "github.com/stolostron/ocm-fleet/poc/provenance-suites/protocol"

// AuthorizedAction is the ordinary delivery value derived by common semantics
// after successful authentication, scope validation, and all required premises.
// Application-owned generation checks may precede its derivation; final
// acceptance and acknowledgement follow successful evaluation.
// Its manifests and basis are immutable once shared within common code.
type AuthorizedAction struct {
	// RootEvidence is the immutable outer evidence identity of the intent.
	RootEvidence protocol.Digest
	// Principal is the successful root verifier's authenticated principal.
	Principal protocol.Principal
	// Scope is the exact authenticated tenant, target, resource, generation,
	// and action validated by the common handler.
	Scope protocol.DeliveryScope
	// PredicateType is the authenticated root's purpose.
	PredicateType protocol.PredicateType
	// Manifests are the final media-typed payloads derived from authenticated content.
	Manifests []protocol.TypedManifest
	// Basis is the sorted unique union of selected evidence identities and
	// their successful normalized validity basis, including retained evidence.
	Basis []protocol.Digest
}

// Clone returns an action with independently owned manifest and basis buffers.
// Use it when crossing an application, queue, or observation ownership boundary.
func (a AuthorizedAction) Clone() AuthorizedAction {
	out := a
	out.Manifests = cloneManifests(a.Manifests)
	out.Basis = cloneSlice(a.Basis)
	return out
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
