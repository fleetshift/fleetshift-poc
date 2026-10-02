package producer

import (
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

func TestProducerRejectsMalformedFullResourceTypes(t *testing.T) {
	user, err := New(Config{Principal: protocol.Principal{
		Scheme: protocol.IdentitySchemeOIDCSubV1, Authority: "https://issuer.example", Subject: "alice",
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, resourceType := range []string{"", "clusters", "/Cluster", "kind.example/", "kind.example/v1/Cluster", " kind.example/Cluster", "kind.example/Clus\tter", "kind.example/Clus\u00a0ter"} {
		t.Run(resourceType, func(t *testing.T) {
			_, err := user.SignManagedResource(context.Background(), protocol.ManagedResourceAuthorization{
				DeliveryScope: protocol.DeliveryScope{TargetID: "target", FullResourceName: "//fleetshift.io/clusters/test", Action: protocol.ActionPut},
				ResourceType:  resourceType,
			})
			if err == nil {
				t.Fatal("signed a malformed managed-resource type")
			}
			_, err = user.SignFulfillmentRelation(context.Background(), protocol.FulfillmentRelation{ResourceType: resourceType, MediaType: "application/json"})
			if err == nil {
				t.Fatal("signed a malformed relation type")
			}
		})
	}
}
