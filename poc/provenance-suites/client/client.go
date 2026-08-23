// Package client models the controlled client. It produces canonical typed
// assertions and uses the selected provenance implementation to create
// evidence. Common client code identifies the allowed provenance type and
// principal authority; it does not obtain an RM-maintained profile ID.
package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

// Config identifies the principal authority and subject this client signs as.
type Config struct {
	Principal protocol.Principal
	TenantID  protocol.TenantID
}

// Client is the controlled-client role.
type Client struct {
	tenantID protocol.TenantID
	profile  *directkey.Client
}

// New constructs a client with a single direct-key/v1 signing key pair.
func New(config Config) (*Client, error) {
	if config.TenantID == "" {
		return nil, errors.New("tenant ID is required")
	}
	profile, err := directkey.NewClient(config.Principal)
	if err != nil {
		return nil, err
	}
	return &Client{tenantID: config.TenantID, profile: profile}, nil
}

// Principal returns the canonical principal this client authenticates as.
func (c *Client) Principal() protocol.Principal {
	return c.profile.Principal()
}

// PublicKey returns the enrollment public key. Delivery evidence does not
// carry this value.
func (c *Client) PublicKey() []byte {
	return c.profile.PublicKey()
}

// ProvenanceType is the allowed provenance type this client will use.
func (c *Client) ProvenanceType() protocol.ProvenanceType {
	return c.profile.ProvenanceType()
}

// DirectKey returns the naive profile implementation for typed lifecycle
// operations such as CreateEnrollment. Fulcio keyless has no equivalent
// enrollment API; that is why enrollment is not a method on Client.
func (c *Client) DirectKey() *directkey.Client {
	return c.profile
}

// SignDeployment creates TypedEvidence for an exact deployment/v1 authorization.
// DeliveryScope.FullResourceName is the client-defined AIP-122 identity of
// the Deployment. TargetID is this POC's static-placement stand-in.
func (c *Client) SignDeployment(ctx context.Context, authorization protocol.DeploymentAuthorization) (protocol.TypedEvidence, error) {
	if err := c.bindScope(&authorization.DeliveryScope); err != nil {
		return protocol.TypedEvidence{}, err
	}
	assertion, err := authorization.Assertion()
	if err != nil {
		return protocol.TypedEvidence{}, err
	}
	return c.profile.CreateEvidence(ctx, assertion)
}

// SignManagedResource creates TypedEvidence for an exact managed-resource/v1
// authorization.
func (c *Client) SignManagedResource(ctx context.Context, authorization protocol.ManagedResourceAuthorization) (protocol.TypedEvidence, error) {
	if err := c.bindScope(&authorization.DeliveryScope); err != nil {
		return protocol.TypedEvidence{}, err
	}
	if authorization.ResourceType == "" {
		return protocol.TypedEvidence{}, errors.New("resource type is required")
	}
	assertion, err := authorization.Assertion()
	if err != nil {
		return protocol.TypedEvidence{}, err
	}
	return c.profile.CreateEvidence(ctx, assertion)
}

// SignFulfillmentRelation creates TypedEvidence for an exact
// fulfillment-relation/v1 assertion. The relation is supporting evidence,
// not a root delivery authorization.
func (c *Client) SignFulfillmentRelation(ctx context.Context, relation protocol.FulfillmentRelation) (protocol.TypedEvidence, error) {
	if relation.ResourceType == "" || relation.MediaType == "" {
		return protocol.TypedEvidence{}, errors.New("resource type and media type are required")
	}
	assertion, err := relation.Assertion()
	if err != nil {
		return protocol.TypedEvidence{}, err
	}
	return c.profile.CreateEvidence(ctx, assertion)
}

func (c *Client) bindScope(scope *protocol.DeliveryScope) error {
	if scope.TenantID == "" {
		scope.TenantID = c.tenantID
	}
	if scope.TenantID != c.tenantID {
		return fmt.Errorf("delivery tenant %q does not match client tenant %q", scope.TenantID, c.tenantID)
	}
	if scope.TargetID == "" || scope.FullResourceName == "" || scope.Action == "" {
		return errors.New("target, resource name, and action are required")
	}
	return nil
}
