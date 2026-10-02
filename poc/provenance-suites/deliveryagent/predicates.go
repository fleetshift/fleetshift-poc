package deliveryagent

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

// The common handlers decode authenticated content once. Their boolean result
// reports whether work is required; completed deliveries derive no new view.
func (a *Agent) handleDeploymentLocked(root verifiedNode) (AppliedDelivery, bool, error) {
	authorization, err := protocol.DecodeDeploymentAuthorization(root.result.Assertion)
	if err != nil {
		return AppliedDelivery{}, false, err
	}
	required, err := a.ordinaryWorkRequiredLocked(root, authorization.DeliveryScope)
	if err != nil || !required {
		return AppliedDelivery{}, false, err
	}
	for i, manifest := range authorization.Manifests {
		if manifest.MediaType == "" {
			return AppliedDelivery{}, false, fmt.Errorf("%w: manifest %d media type is required", protocol.ErrMalformedEvidence, i)
		}
	}
	return AppliedDelivery{
		Scope:         authorization.DeliveryScope,
		PredicateType: protocol.PredicateTypeDeploymentV1,
		Manifests:     authorization.Manifests,
	}, true, nil
}

func (a *Agent) handleManagedResourceLocked(session *verificationSession, root verifiedNode) (AppliedDelivery, bool, error) {
	authorization, err := protocol.DecodeManagedResourceAuthorization(root.result.Assertion)
	if err != nil {
		return AppliedDelivery{}, false, err
	}
	required, err := a.ordinaryWorkRequiredLocked(root, authorization.DeliveryScope)
	if err != nil || !required {
		return AppliedDelivery{}, false, err
	}
	relation, err := a.verifyFulfillmentRelationLocked(session, root.identity, authorization)
	if err != nil {
		return AppliedDelivery{}, false, err
	}
	// RegisteredSelfTarget: the named delivery target is the addon itself.
	// TargetID is this POC's static-placement stand-in; early checks have
	// already required it to equal this agent's ID before supporting work.
	return AppliedDelivery{
		Scope:         authorization.DeliveryScope,
		PredicateType: protocol.PredicateTypeManagedResourceV1,
		Manifests: []protocol.TypedManifest{{
			MediaType: relation.MediaType,
			Bytes:     authorization.Spec,
		}},
	}, true, nil
}

// ordinaryWorkRequiredLocked checks the decoded intent before supporting work
// or manifest derivation. Authentication is complete even for completed-key
// no-ops. The agent lock fences this decision and subsequent effects together.
func (a *Agent) ordinaryWorkRequiredLocked(root verifiedNode, scope protocol.DeliveryScope) (bool, error) {
	if scope.Tenant.Scheme == "" || scope.Tenant.Authority == "" || scope.TargetID == "" || scope.FullResourceName == "" || scope.Action == "" {
		return false, fmt.Errorf("%w: tenant, target, resource name, and action are required", protocol.ErrMalformedEvidence)
	}
	if scope.Action != protocol.ActionPut && scope.Action != protocol.ActionRemove {
		return false, fmt.Errorf("%w: unsupported action %q", protocol.ErrMalformedEvidence, scope.Action)
	}
	if scope.Tenant != a.config.Tenant || scope.TargetID != a.config.TargetID {
		return false, fmt.Errorf("%w: tenant or target mismatch", protocol.ErrPolicyReevaluation)
	}
	if root.result.Authenticated.Principal.Tenant() != a.config.Tenant {
		return false, fmt.Errorf("%w: root principal does not belong to the provisioned resource tenant", protocol.ErrTenantMismatch)
	}
	previous, exists := a.generations[scope.FullResourceName]
	if exists {
		if scope.Generation < previous {
			return false, fmt.Errorf("%w: generation %d is older than %d", ErrGeneration, scope.Generation, previous)
		}
		if scope.Generation == previous {
			return false, nil
		}
	}
	return true, nil
}

func (a *Agent) verifyFulfillmentRelationLocked(session *verificationSession, parent protocol.Digest, authorization protocol.ManagedResourceAuthorization) (protocol.FulfillmentRelation, error) {
	if a.config.ProviderTenant == (protocol.Tenant{}) {
		return protocol.FulfillmentRelation{}, fmt.Errorf("%w: provider tenant is not provisioned", protocol.ErrTenantMismatch)
	}
	identity, err := session.supportingRelation(authorization.ResourceType)
	if err != nil {
		return protocol.FulfillmentRelation{}, err
	}

	var relation protocol.FulfillmentRelation
	err = session.withNode(
		context.Background(),
		identity,
		func(support verifiedNode) error {
			if support.result.Authenticated.PredicateType != protocol.PredicateTypeFulfillmentRelationV1 {
				return fmt.Errorf("%w: %s", protocol.ErrUnknownPredicateType, support.result.Authenticated.PredicateType)
			}
			if support.result.Authenticated.Principal.Tenant() != a.config.ProviderTenant {
				return fmt.Errorf("%w: fulfillment relation principal does not belong to the platform provider tenant", protocol.ErrTenantMismatch)
			}
			decoded, err := protocol.DecodeFulfillmentRelation(support.result.Assertion)
			if err != nil {
				return err
			}
			if decoded.MediaType == "" {
				return fmt.Errorf("%w: fulfillment relation media type is required", protocol.ErrMalformedEvidence)
			}
			if decoded.ResourceType != authorization.ResourceType {
				return fmt.Errorf("%w: fulfillment relation resource type %q, authorization %q", protocol.ErrPolicyReevaluation, decoded.ResourceType, authorization.ResourceType)
			}
			if err := session.recordDependency(parent, support.identity); err != nil {
				return err
			}
			relation = decoded
			return nil
		},
	)
	if err != nil {
		return protocol.FulfillmentRelation{}, err
	}
	return relation, nil
}
