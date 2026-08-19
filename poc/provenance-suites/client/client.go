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

// SignDelivery creates TypedEvidence for an exact delivery authorization.
func (c *Client) SignDelivery(ctx context.Context, authorization protocol.DeliveryAuthorization) (protocol.TypedEvidence, protocol.TypedAssertion, error) {
	if authorization.TenantID == "" {
		authorization.TenantID = c.tenantID
	}
	if authorization.TenantID != c.tenantID {
		return protocol.TypedEvidence{}, protocol.TypedAssertion{}, fmt.Errorf("delivery tenant %q does not match client tenant %q", authorization.TenantID, c.tenantID)
	}
	if authorization.TargetID == "" || authorization.FulfillmentID == "" || authorization.Action == "" {
		return protocol.TypedEvidence{}, protocol.TypedAssertion{}, errors.New("target, fulfillment, and action are required")
	}
	assertion, err := authorization.Assertion()
	if err != nil {
		return protocol.TypedEvidence{}, protocol.TypedAssertion{}, err
	}
	evidence, err := c.profile.CreateEvidence(ctx, assertion)
	if err != nil {
		return protocol.TypedEvidence{}, protocol.TypedAssertion{}, err
	}
	return evidence, assertion, nil
}
