package producer

import (
	"bytes"
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

func TestSignedScopeUsesExternalTenantIdentity(t *testing.T) {
	principal := protocol.Principal{Scheme: protocol.IdentitySchemeOIDCSubV1, Authority: "https://issuer.example", TenantPartition: "external-acme", Subject: "alice"}
	user, err := New(Config{Principal: principal})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := user.SignDeployment(context.Background(), protocol.DeploymentAuthorization{
		DeliveryScope: protocol.DeliveryScope{TargetID: "target-east", FullResourceName: "//fleetshift.io/deployments/web", Generation: 1, Action: protocol.ActionPut},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertion, err := directkey.NewManager().DecodeAssertion(evidence)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := protocol.DecodeDeliveryScope(assertion)
	if err != nil {
		t.Fatal(err)
	}
	if scope.Tenant != principal.Tenant() {
		t.Fatalf("scope tenant = %+v, want %+v", scope.Tenant, principal.Tenant())
	}
	if bytes.Contains(assertion.Bytes, []byte(`"tenant_id"`)) || bytes.Contains(evidence.Bytes, []byte("private-routing-id")) {
		t.Fatal("signed delivery scope exposes an internal tenant ID")
	}
}

func TestProducerRejectsAnotherTenantInDeliveryScope(t *testing.T) {
	principal := protocol.Principal{Scheme: protocol.IdentitySchemeOIDCSubV1, Authority: "https://issuer.example", TenantPartition: "acme", Subject: "alice"}
	user, err := New(Config{Principal: principal})
	if err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []protocol.Tenant{
		{PrincipalAuthority: principal.PrincipalAuthority(), Partition: "other"},
		{PrincipalAuthority: protocol.PrincipalAuthority{Scheme: principal.Scheme, Authority: "https://another.example"}, Partition: "acme"},
	} {
		_, err := user.SignDeployment(context.Background(), protocol.DeploymentAuthorization{DeliveryScope: protocol.DeliveryScope{
			Tenant: tenant, TargetID: "target-east", FullResourceName: "//fleetshift.io/deployments/web", Generation: 1, Action: protocol.ActionPut,
		}})
		if err == nil {
			t.Fatalf("accepted another tenant %+v", tenant)
		}
	}
}
