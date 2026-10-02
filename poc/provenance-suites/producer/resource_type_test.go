package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

func TestProducerRejectsUnconstructedResourceTypesBeforeSigning(t *testing.T) {
	user, err := New(Config{Principal: protocol.Principal{Scheme: protocol.IdentitySchemeOIDCSubV1, Authority: "https://issuer.example", Subject: "alice"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = user.SignManagedResource(context.Background(), protocol.ManagedResourceAuthorization{
		DeliveryScope: protocol.DeliveryScope{TargetID: "target", FullResourceName: "resources.example/test", Action: protocol.ActionPut},
	})
	if !errors.Is(err, protocol.ErrMalformedEvidence) {
		t.Fatalf("unconstructed managed-resource type: %v", err)
	}
	_, err = user.SignFulfillmentRelation(context.Background(), protocol.FulfillmentRelation{MediaType: "application/json"})
	if !errors.Is(err, protocol.ErrMalformedEvidence) {
		t.Fatalf("unconstructed relation type: %v", err)
	}
}
