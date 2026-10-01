package provenancesuites

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/deliveryagent"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/producer"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/resourcemanager"
)

func TestPoliciesControlLoggingAcrossConsumerAndProviderTenants(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		rootLog, relationLog bool
		defaultsFirst        bool
	}{
		{"neither", false, false, false}, {"consumer only", true, false, false},
		{"provider only", false, true, false}, {"both", true, true, false},
		{"defaults first neither", false, false, true}, {"defaults first consumer only", true, false, true},
		{"defaults first provider only", false, true, true}, {"defaults first both", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			consumerPartition, providerPartition := protocol.TenantPartition("external-consumer"), protocol.TenantPartition("external-provider")
			consumerTenant := protocol.Tenant{PrincipalAuthority: protocol.PrincipalAuthority{Scheme: protocol.IdentitySchemeOIDCSubV1, Authority: testIssuer}, Partition: consumerPartition}
			profile := protocol.ProfileConfig{ProvenanceType: protocol.ProvenanceTypeDirectKeyV1}
			makePolicy := func(predicate protocol.PredicateType, partition *protocol.TenantPartition, log bool) protocol.DeliveryPolicy {
				return protocol.DeliveryPolicy{Match: protocol.PolicyMatch{PredicateType: predicate, TenantPartition: partition},
					Provenance: protocol.RequirementRequired, LiveCredential: protocol.RequirementNone,
					Profiles: []protocol.Digest{profileDigest(profile)}, RequireEvidenceLog: log}
			}
			// Each later matching policy has the opposite logging requirement.
			// RM registration and target verification must both use only the first.
			rootPartition, relationPartition := &consumerPartition, &providerPartition
			var laterRootPartition, laterRelationPartition *protocol.TenantPartition
			if tc.defaultsFirst {
				rootPartition, relationPartition = nil, nil
				laterRootPartition, laterRelationPartition = &consumerPartition, &providerPartition
			}
			trust := protocol.TrustConfiguration{AuthorityRegistry: []protocol.AuthorityConfig{{
				PrincipalAuthority: protocol.PrincipalAuthority{Scheme: protocol.IdentitySchemeOIDCSubV1, Authority: testIssuer},
				ProvenanceProfiles: []protocol.ProfileConfig{profile}, DeliveryPolicies: []protocol.DeliveryPolicy{
					makePolicy(directkey.PredicateTypeEnrollmentV1, nil, false),
					makePolicy(protocol.PredicateTypeManagedResourceV1, rootPartition, tc.rootLog),
					makePolicy(protocol.PredicateTypeFulfillmentRelationV1, relationPartition, tc.relationLog),
					makePolicy(protocol.PredicateTypeManagedResourceV1, laterRootPartition, !tc.rootLog),
					makePolicy(protocol.PredicateTypeFulfillmentRelationV1, laterRelationPartition, !tc.relationLog),
				},
			}}}
			agent, err := deliveryagent.New(deliveryagent.Config{Tenant: protocol.Tenant{PrincipalAuthority: trust.AuthorityRegistry[0].PrincipalAuthority, Partition: consumerPartition}, ProviderTenant: protocol.Tenant{PrincipalAuthority: trust.AuthorityRegistry[0].PrincipalAuthority, Partition: providerPartition}, TargetID: testTarget})
			if err != nil {
				t.Fatal(err)
			}
			if err := agent.Bootstrap(trust); err != nil {
				t.Fatal(err)
			}
			manager, err := resourcemanager.New(resourcemanager.Config{TenantID: "routing-consumer", Tenant: consumerTenant, Trust: trust}, nil)
			if err != nil {
				t.Fatal(err)
			}
			recording := &packageRecordingAgent{delegate: agent}
			if err := manager.RegisterAgent(testTarget, recording); err != nil {
				t.Fatal(err)
			}
			makeProducer := func(partition protocol.TenantPartition, subject protocol.Subject) *producer.Producer {
				p, err := producer.New(producer.Config{Principal: protocol.Principal{
					Scheme: protocol.IdentitySchemeOIDCSubV1, Authority: testIssuer, TenantPartition: partition, Subject: subject,
				}})
				if err != nil {
					t.Fatal(err)
				}
				enrollment, err := p.DirectKey().CreateEnrollment()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := manager.SubmitDirectKeyEnrollment(context.Background(), p.Principal(), enrollment); err != nil {
					t.Fatal(err)
				}
				return p
			}
			consumer := makeProducer(consumerPartition, "alice")
			provider := makeProducer(providerPartition, "alice")
			if manager.EvidenceLogSize() != 0 {
				t.Fatal("optional enrollments were logged")
			}
			root, err := consumer.SignManagedResource(context.Background(), protocol.ManagedResourceAuthorization{
				DeliveryScope: protocol.DeliveryScope{TargetID: testTarget, FullResourceName: clusterName("cross-tenant"), Generation: 1, Action: protocol.ActionPut},
				ResourceType:  testResourceType, Spec: []byte(`{"replicas":2}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			relation, err := provider.SignFulfillmentRelation(context.Background(), protocol.FulfillmentRelation{ResourceType: testResourceType, MediaType: testReplicasMediaType})
			if err != nil {
				t.Fatal(err)
			}
			agent.LoseNextAcknowledgement()
			receipt, err := manager.SubmitDelivery(context.Background(), consumer.Principal(), root, relation)
			if err != deliveryagent.ErrAcknowledgementLost && !errors.Is(err, deliveryagent.ErrAcknowledgementLost) {
				t.Fatalf("lost acknowledgement error = %v", err)
			}
			if err := manager.Dispatch(context.Background(), onlyDispatch(t, receipt)); err != nil {
				t.Fatal(err)
			}
			wantLeaves := uint64(0)
			if tc.rootLog {
				wantLeaves++
			}
			if tc.relationLog {
				wantLeaves++
			}
			if got := manager.EvidenceLogSize(); got != wantLeaves {
				t.Fatalf("log size = %d, want %d", got, wantLeaves)
			}
			rootID, _ := root.Identity()
			relationID, _ := relation.Identity()
			if _, logged := manager.EvidenceLogIndex(rootID); logged != tc.rootLog {
				t.Fatal("root log registration disagrees with consumer policy")
			}
			if _, logged := manager.EvidenceLogIndex(relationID); logged != tc.relationLog {
				t.Fatal("relation log registration disagrees with provider policy")
			}
			pkg := recording.last
			if (pkg.Root.EvidenceLog != nil) != tc.rootLog || (pkg.Supporting[0].EvidenceLog != nil) != tc.relationLog {
				t.Fatal("couriered inclusions disagree with policy")
			}
			if (pkg.EvidenceLog != nil) != (tc.rootLog || tc.relationLog) {
				t.Fatal("package log update presence disagrees with registered items")
			}
			if _, ok := agent.Applied(clusterName("cross-tenant")); !ok {
				t.Fatal("cross-tenant managed resource was not applied")
			}
		})
	}
}

type packageRecordingAgent struct {
	delegate resourcemanager.DeliveryAgent
	last     resourcemanager.DeliveryPackage
}

func (a *packageRecordingAgent) Deliver(pkg resourcemanager.DeliveryPackage) error {
	a.last = pkg
	return a.delegate.Deliver(pkg)
}
