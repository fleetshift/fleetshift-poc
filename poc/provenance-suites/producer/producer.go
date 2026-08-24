// Package producer models the controlled producer. It produces canonical typed
// assertions and uses the selected provenance implementation to create
// evidence. Common producer code identifies the allowed provenance type and
// principal authority; it does not obtain an RM-maintained profile ID.
package producer

import (
	"context"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

// Config identifies the principal authority and subject this producer signs as.
type Config struct {
	Principal protocol.Principal
	TenantID  protocol.TenantID
}

// Producer is the controlled-producer role.
type Producer struct {
	tenantID protocol.TenantID
	profile  *directkey.Producer
}

// New constructs a producer with a single direct-key/v1 signing key pair.
func New(config Config) (*Producer, error) {
	if config.TenantID == "" {
		return nil, errors.New("tenant ID is required")
	}
	profile, err := directkey.NewProducer(config.Principal)
	if err != nil {
		return nil, err
	}
	return &Producer{tenantID: config.TenantID, profile: profile}, nil
}

// Principal returns the canonical principal this producer authenticates as.
func (p *Producer) Principal() protocol.Principal {
	return p.profile.Principal()
}

// PublicKey returns the enrollment public key. Delivery evidence does not
// carry this value.
func (p *Producer) PublicKey() []byte {
	return p.profile.PublicKey()
}

// ProvenanceType is the allowed provenance type this producer will use.
func (p *Producer) ProvenanceType() protocol.ProvenanceType {
	return p.profile.ProvenanceType()
}

// DirectKey returns the naive profile implementation for typed lifecycle
// operations such as CreateEnrollment. Fulcio keyless has no equivalent
// enrollment API; that is why enrollment is not a method on Producer.
func (p *Producer) DirectKey() *directkey.Producer {
	return p.profile
}

// SignDeployment creates TypedEvidence for an exact deployment/v1 authorization.
// DeliveryScope.FullResourceName is the producer-defined AIP-122 identity of
// the Deployment. TargetID is this POC's static-placement stand-in.
func (p *Producer) SignDeployment(ctx context.Context, authorization protocol.DeploymentAuthorization) (protocol.TypedEvidence, error) {
	if err := p.bindScope(&authorization.DeliveryScope); err != nil {
		return protocol.TypedEvidence{}, err
	}
	assertion, err := authorization.Assertion()
	if err != nil {
		return protocol.TypedEvidence{}, err
	}
	return p.profile.CreateEvidence(ctx, assertion)
}

// SignManagedResource creates TypedEvidence for an exact managed-resource/v1
// authorization.
func (p *Producer) SignManagedResource(ctx context.Context, authorization protocol.ManagedResourceAuthorization) (protocol.TypedEvidence, error) {
	if err := p.bindScope(&authorization.DeliveryScope); err != nil {
		return protocol.TypedEvidence{}, err
	}
	if authorization.ResourceType == "" {
		return protocol.TypedEvidence{}, errors.New("resource type is required")
	}
	assertion, err := authorization.Assertion()
	if err != nil {
		return protocol.TypedEvidence{}, err
	}
	return p.profile.CreateEvidence(ctx, assertion)
}

// SignFulfillmentRelation creates TypedEvidence for an exact
// fulfillment-relation/v1 assertion. The relation is supporting evidence,
// not a root delivery authorization.
func (p *Producer) SignFulfillmentRelation(ctx context.Context, relation protocol.FulfillmentRelation) (protocol.TypedEvidence, error) {
	if relation.ResourceType == "" || relation.MediaType == "" {
		return protocol.TypedEvidence{}, errors.New("resource type and media type are required")
	}
	assertion, err := relation.Assertion()
	if err != nil {
		return protocol.TypedEvidence{}, err
	}
	return p.profile.CreateEvidence(ctx, assertion)
}

func (p *Producer) bindScope(scope *protocol.DeliveryScope) error {
	if scope.TenantID == "" {
		scope.TenantID = p.tenantID
	}
	if scope.TenantID != p.tenantID {
		return fmt.Errorf("delivery tenant %q does not match producer tenant %q", scope.TenantID, p.tenantID)
	}
	if scope.TargetID == "" || scope.FullResourceName == "" || scope.Action == "" {
		return errors.New("target, resource name, and action are required")
	}
	return nil
}
