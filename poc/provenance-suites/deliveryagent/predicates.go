package deliveryagent

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

// The common handlers decode authenticated content once. Their boolean result
// reports whether work is required; completed deliveries derive no action.
func (a *Verifier) handleDeployment(ctx context.Context, session *verificationSession, root verifiedNode, precheck GenerationPrecheck) (AuthorizedAction, bool, error) {
	authorization, err := protocol.DecodeDeploymentAuthorization(root.result.Assertion)
	if err != nil {
		return AuthorizedAction{}, false, err
	}
	required, err := a.ordinaryWorkRequired(ctx, root, authorization.DeliveryScope, precheck)
	if err != nil || !required {
		return AuthorizedAction{}, false, err
	}
	session.addBasisContribution(root)
	for i, manifest := range authorization.Manifests {
		if manifest.MediaType == "" {
			return AuthorizedAction{}, false, fmt.Errorf("%w: manifest %d media type is required", protocol.ErrMalformedEvidence, i)
		}
	}
	return session.authorizedAction(root, authorization.DeliveryScope, authorization.Manifests), true, nil
}

func (a *Verifier) handleManagedResource(ctx context.Context, session *verificationSession, root verifiedNode, precheck GenerationPrecheck) (AuthorizedAction, bool, error) {
	authorization, err := protocol.DecodeManagedResourceAuthorization(root.result.Assertion)
	if err != nil {
		return AuthorizedAction{}, false, err
	}
	required, err := a.ordinaryWorkRequired(ctx, root, authorization.DeliveryScope, precheck)
	if err != nil || !required {
		return AuthorizedAction{}, false, err
	}
	session.addBasisContribution(root)
	relation, err := a.verifyFulfillmentRelation(ctx, session, root.identity, authorization)
	if err != nil {
		return AuthorizedAction{}, false, err
	}
	// RegisteredSelfTarget: the named delivery target is the addon itself.
	// TargetID is this POC's static-placement stand-in; early checks have
	// already required it to equal this agent's ID before supporting work.
	manifests := []protocol.TypedManifest{{
		MediaType: relation.MediaType,
		Bytes:     authorization.Spec,
	}}
	return session.authorizedAction(root, authorization.DeliveryScope, manifests), true, nil
}

// ordinaryWorkRequired validates common scope before any application query.
// The optional precheck supplies only a progress observation or transient guard;
// application-owned acceptance still enforces generation ordering with effects.
func (a *Verifier) ordinaryWorkRequired(ctx context.Context, root verifiedNode, scope protocol.DeliveryScope, precheck GenerationPrecheck) (bool, error) {
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
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if precheck != nil {
		completed, err := precheck(ctx, scope)
		if err != nil {
			return false, err
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return !completed, nil
	}
	return true, nil
}

func (a *Verifier) verifyFulfillmentRelation(ctx context.Context, session *verificationSession, parent protocol.Digest, authorization protocol.ManagedResourceAuthorization) (protocol.FulfillmentRelation, error) {
	if a.config.ProviderTenant == (protocol.Tenant{}) {
		return protocol.FulfillmentRelation{}, fmt.Errorf("%w: provider tenant is not provisioned", protocol.ErrTenantMismatch)
	}
	identity, err := session.supportingRelation(authorization.ResourceType)
	if err != nil {
		return protocol.FulfillmentRelation{}, err
	}

	var relation protocol.FulfillmentRelation
	err = session.withNode(
		ctx,
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
