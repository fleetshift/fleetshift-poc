package deliveryagent

import (
	"context"
	"fmt"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/temporal"
)

func TestRequestedRelationUsesItsOwnInstalledImplementationAndPolicy(t *testing.T) {
	user := testProducer(t, "user")
	providerPrincipal := user.Principal()
	providerPrincipal.Subject = "provider-addon"
	providerPrincipal.TenantPartition = "provider"
	addon, err := directkey.NewProducer(providerPrincipal)
	if err != nil {
		t.Fatal(err)
	}
	rootTarget, relationTarget := directkey.NewTarget(), directkey.NewTarget()
	enrollTestTarget(t, rootTarget, user)
	enrollTestTarget(t, relationTarget, addon)
	rootCounted := &countedTarget{delegate: rootTarget}
	alternate := &relationTestProfile{TargetAPI: relationTarget}
	relationCounted := &countedTarget{delegate: alternate}
	root := lookupManagedRoot(t, user, "kind.example/Cluster")
	support := lookupRelationItem(t, addon, "kind.example/Cluster", "application/json")
	support.Evidence.ProvenanceType = alternate.ProvenanceType()
	pkg := loggedTestPackage(t, root, support.Evidence)
	catalog, err := newEvidenceCatalog(pkg, catalogLookup(map[protocol.ProvenanceType]protocol.TargetAPI{
		rootCounted.ProvenanceType():     rootCounted,
		relationCounted.ProvenanceType(): relationCounted,
	}), defaultVerificationLimits())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := temporal.Prepare(temporal.RetainedState{EvidenceLog: protocol.EmptyCheckpoint()}, catalog.update, root, pkg.Root.EvidenceLog)
	if err != nil {
		t.Fatal(err)
	}
	trust := sessionTestTrust()
	profile := protocol.ProfileConfig{ProvenanceType: alternate.ProvenanceType()}
	authority := &trust.AuthorityRegistry[0]
	authority.ProvenanceProfiles = append(authority.ProvenanceProfiles, profile)
	authority.DeliveryPolicies[1] = sessionPolicy(protocol.PredicateTypeFulfillmentRelationV1, profile)
	providerPartition := providerPrincipal.TenantPartition
	authority.DeliveryPolicies[1].Match.TenantPartition = &providerPartition
	authority.DeliveryPolicies = append(authority.DeliveryPolicies, sessionPolicy(protocol.PredicateTypeManagedResourceV1, authority.ProvenanceProfiles[0]))
	session := newVerificationSession(catalog, trust, protocol.TemporalVerificationServices{Log: prepared.Log})
	agent := newDispatchTestAgent()
	agent.config.ProviderTenant = providerPrincipal.Tenant()
	if err := session.withNode(context.Background(), catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) }); err != nil {
		t.Fatal(err)
	}
	if _, ok := agent.Applied("//fleetshift.io/clusters/test"); !ok {
		t.Fatal("mixed provenance delivery did not apply")
	}
	if rootCounted.beginCalls != 1 || relationCounted.beginCalls != 1 || relationCounted.finishCalls != 1 || session.edgeCount != 1 {
		t.Fatalf("root begin=%d relation begin=%d finish=%d edges=%d", rootCounted.beginCalls, relationCounted.beginCalls, relationCounted.finishCalls, session.edgeCount)
	}
	if relationCounted.requests[0].DeliveryContext.TenantPartition != providerPartition || relationCounted.requests[0].ProfileConfig.ProvenanceType != profile.ProvenanceType {
		t.Fatal("relation verification used the root's policy context")
	}
	assertSelectedBasis(t, session, catalog.rootID, catalog.supporting[0])
}

// relationTestProfile uses real direct-key signatures and retained keys under a
// second configured type to exercise common composition without implementing
// another production suite. It adapts only native envelope/configuration tags;
// all signature checks still run in the real direct-key session.
type relationTestProfile struct{ protocol.TargetAPI }

func (*relationTestProfile) ProvenanceType() protocol.ProvenanceType { return "test-relation/v1" }

func (p *relationTestProfile) ParseHints(evidence protocol.TypedEvidence) (protocol.TentativeHints, error) {
	if evidence.ProvenanceType != p.ProvenanceType() {
		return protocol.TentativeHints{}, fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, evidence.ProvenanceType)
	}
	evidence.ProvenanceType = p.TargetAPI.ProvenanceType()
	return p.TargetAPI.ParseHints(evidence)
}

func (p *relationTestProfile) BeginVerification(ctx context.Context, request protocol.VerifyRequest) (protocol.ProvenanceVerificationSession, error) {
	if request.ProfileConfig.ProvenanceType != p.ProvenanceType() || request.Statement.Evidence.ProvenanceType != p.ProvenanceType() {
		return nil, protocol.ErrUnknownProvenanceType
	}
	profileDigest, err := request.ProfileConfig.Digest()
	if err != nil {
		return nil, err
	}
	request.ProfileConfig.ProvenanceType = p.TargetAPI.ProvenanceType()
	request.Statement.Evidence.ProvenanceType = p.TargetAPI.ProvenanceType()
	session, err := p.TargetAPI.BeginVerification(ctx, request)
	if err != nil {
		return nil, err
	}
	return &relationTestProfileSession{ProvenanceVerificationSession: session, kind: p.ProvenanceType(), profile: profileDigest}, nil
}

type relationTestProfileSession struct {
	protocol.ProvenanceVerificationSession
	kind    protocol.ProvenanceType
	profile protocol.Digest
}

func (s *relationTestProfileSession) Finish(ctx context.Context, inputs protocol.VerifiedProvenanceTemporalInputs) (protocol.ProvenanceAuthenticationResult, error) {
	result, err := s.ProvenanceVerificationSession.Finish(ctx, inputs)
	if err != nil {
		return protocol.ProvenanceAuthenticationResult{}, err
	}
	result.Authenticated.ProvenanceType = s.kind
	result.Authenticated.ProfileConfigDigest = s.profile
	return result, nil
}
