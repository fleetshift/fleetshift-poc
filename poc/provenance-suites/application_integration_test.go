package provenancesuites

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/deliveryagent"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/producer"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

func TestCommonVerifierHasNoApplicationEffects(t *testing.T) {
	verifier, user, addon := applicationVerifier(t)
	root := mustSignManagedResource(t, user, clusterName("evaluation"), 1, []byte(`{"region":"east"}`))
	relation := mustSignRelation(t, addon, testResourceType, testClusterSpecMediaType)
	var result deliveryagent.VerifiedDelivery
	result, err := verifier.Verify(context.Background(), applicationPackage(root, relation), nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Action == nil || result.Completed || result.SuiteEvent != nil {
		t.Fatalf("ordinary evaluation: %+v", result)
	}
	rootID, _ := root.Identity()
	relationID, _ := relation.Identity()
	if result.Action.RootEvidence != rootID || result.Action.Principal != user.Principal() || len(result.Action.Basis) != 2 {
		t.Fatal("evaluation lost authenticated action facts")
	}
	application := deliveryagent.NewMemoryApplication()
	if _, exists := application.Applied(clusterName("evaluation")); exists {
		t.Fatal("evaluation performed application effects")
	}
	attempt := application.NewAttempt()
	defer attempt.Close()
	if err := attempt.Accept(context.Background(), *result.Action); err != nil {
		t.Fatal(err)
	}
	view, ok := application.Applied(clusterName("evaluation"))
	if !ok || !reflect.DeepEqual(view.AuthorizedAction, *result.Action) || (view.Basis[0] != relationID && view.Basis[1] != relationID) {
		t.Fatal("an independent application did not receive the complete action")
	}
	result.Action.Manifests[0].Bytes[0] ^= 1
	result.Action.Basis[0] = "caller mutation"
	again, _ := application.Applied(clusterName("evaluation"))
	if !reflect.DeepEqual(view, again) {
		t.Fatal("application retained caller-owned action buffers")
	}
}

func TestApplicationPrecheckIsOptionalAndInvalidRetryDoesNotChangeCompletion(t *testing.T) {
	for _, early := range []bool{true, false} {
		t.Run(map[bool]string{true: "early", false: "deferred"}[early], func(t *testing.T) {
			verifier, user, addon := applicationVerifier(t)
			memory := deliveryagent.NewMemoryApplication()
			var application deliveryagent.Application = memory
			if !early {
				application = lateApplication{memory}
			}
			agent, err := deliveryagent.NewAgent(verifier, application)
			if err != nil {
				t.Fatal(err)
			}
			root := mustSignManagedResource(t, user, clusterName("retry"), 1, []byte(`{"region":"east"}`))
			relation := mustSignRelation(t, addon, testResourceType, testClusterSpecMediaType)
			if err := agent.Deliver(applicationPackage(root, relation)); err != nil {
				t.Fatal(err)
			}
			before, _ := memory.Applied(clusterName("retry"))
			err = agent.Deliver(applicationPackage(root))
			if early && err != nil || !early && !errors.Is(err, deliveryagent.ErrFulfillmentRelationRequired) {
				t.Fatalf("retry: %v", err)
			}
			after, _ := memory.Applied(clusterName("retry"))
			if !reflect.DeepEqual(before, after) {
				t.Fatal("invalid retry changed the completed action")
			}
			if !early {
				if err := agent.Deliver(applicationPackage(root, relation)); err != nil {
					t.Fatal(err)
				}
				after, _ = memory.Applied(clusterName("retry"))
				if !reflect.DeepEqual(before, after) {
					t.Fatal("late completed-generation check replaced the accepted action")
				}
			}
		})
	}
}

func TestApplicationRechecksGenerationAfterEvaluation(t *testing.T) {
	verifier, user, _ := applicationVerifier(t)
	memory := deliveryagent.NewMemoryApplication()
	older := mustSignDeployment(t, user, deploymentName("fence"), 1, []byte(`{"replicas":1}`))
	newer := mustSignDeployment(t, user, deploymentName("fence"), 2, []byte(`{"replicas":2}`))
	attempt := memory.NewAttempt()
	defer attempt.Close()
	precheck := attempt.(deliveryagent.GenerationPrechecker).Precheck
	first, err := verifier.Verify(context.Background(), applicationPackage(older), precheck)
	if err != nil {
		t.Fatal(err)
	}
	second, err := verifier.Verify(context.Background(), applicationPackage(newer), nil)
	if err != nil {
		t.Fatal(err)
	}
	other := memory.NewAttempt()
	defer other.Close()
	if err := other.Accept(context.Background(), *second.Action); err != nil {
		t.Fatal(err)
	}
	if err := attempt.Accept(context.Background(), *first.Action); !errors.Is(err, deliveryagent.ErrGeneration) {
		t.Fatalf("older action after another writer advanced generation: %v", err)
	}
	view, _ := memory.Applied(deploymentName("fence"))
	if !reflect.DeepEqual(view.AuthorizedAction, *second.Action) {
		t.Fatal("the older action overwrote newer effects")
	}
}

func TestAgentAcknowledgesOnlyEvaluatedSafelyAcceptedWork(t *testing.T) {
	verifier, user, addon := applicationVerifier(t)
	application := newQueuedApplication()
	agent, err := deliveryagent.NewAgent(verifier, application)
	if err != nil {
		t.Fatal(err)
	}
	root := mustSignManagedResource(t, user, clusterName("queued"), 1, []byte(`{"region":"east"}`))
	relation := mustSignRelation(t, addon, testResourceType, testClusterSpecMediaType)
	if err := agent.Deliver(applicationPackage(root)); !errors.Is(err, deliveryagent.ErrFulfillmentRelationRequired) {
		t.Fatalf("incomplete evidence: %v", err)
	}
	if application.accepts != 0 || application.closes != 1 {
		t.Fatal("evaluation failure reached acceptance or leaked its attempt")
	}
	application.failNext = errors.New("acceptance storage failed")
	agent.LoseNextAcknowledgement()
	if err := agent.Deliver(applicationPackage(root, relation)); !errors.Is(err, application.failNext) {
		t.Fatalf("failed acceptance: %v", err)
	}
	if len(application.pending) != 0 || application.closes != 2 {
		t.Fatal("failed acceptance recorded work or leaked its attempt")
	}
	application.failNext = nil
	if err := agent.Deliver(applicationPackage(root, relation)); !errors.Is(err, deliveryagent.ErrAcknowledgementLost) {
		t.Fatalf("ack after safe acceptance: %v", err)
	}
	if _, exists := application.effects.Applied(clusterName("queued")); exists || len(application.pending) != 1 {
		t.Fatal("the queued application did not acknowledge before effects")
	}
	// In-progress acceptance is not completion. An incomplete retry still needs
	// supporting authorization; it cannot use the completed-work shortcut.
	if err := agent.Deliver(applicationPackage(root)); !errors.Is(err, deliveryagent.ErrFulfillmentRelationRequired) {
		t.Fatalf("in-progress retry skipped evaluation: %v", err)
	}
	if err := agent.Deliver(applicationPackage(root, relation)); err != nil {
		t.Fatal(err)
	}
	if len(application.pending) != 1 {
		t.Fatal("accepted retry duplicated pending work")
	}
	if err := application.finish(clusterName("queued")); err != nil {
		t.Fatal(err)
	}
	if _, exists := application.effects.Applied(clusterName("queued")); !exists {
		t.Fatal("acknowledged work could not finish from its retained action")
	}
}

func TestQueuedAcceptanceFencesOlderEffectsBeforeNewerEffectsBegin(t *testing.T) {
	verifier, user, _ := applicationVerifier(t)
	application := newQueuedApplication()
	agent, err := deliveryagent.NewAgent(verifier, application)
	if err != nil {
		t.Fatal(err)
	}
	name := deploymentName("queued-fence")
	older := mustSignDeployment(t, user, name, 1, []byte(`{"replicas":1}`))
	newer := mustSignDeployment(t, user, name, 2, []byte(`{"replicas":2}`))
	if err := agent.Deliver(applicationPackage(older)); err != nil {
		t.Fatal(err)
	}
	capturedOldWork := application.pending[name].Clone()
	if err := agent.Deliver(applicationPackage(newer)); err != nil {
		t.Fatal(err)
	}
	if _, exists := application.effects.Applied(name); exists {
		t.Fatal("queued acceptance performed effects")
	}
	if err := application.finishAction(capturedOldWork); !errors.Is(err, deliveryagent.ErrGeneration) {
		t.Fatalf("older queued execution after newer acceptance: %v", err)
	}
	if err := agent.Deliver(applicationPackage(older)); !errors.Is(err, deliveryagent.ErrGeneration) {
		t.Fatalf("stale retry against accepted high-water mark: %v", err)
	}
	if err := application.finish(name); err != nil {
		t.Fatal(err)
	}
	view, ok := application.effects.Applied(name)
	if !ok || view.Scope.Generation != 2 {
		t.Fatal("accepted newer action did not finish from retained work")
	}
}

func TestFailedEvaluationReleasesAnEarlyGuardWithoutAdmittingGeneration(t *testing.T) {
	verifier, user, _ := applicationVerifier(t)
	application := newQueuedApplication()
	application.guard = true
	agent, err := deliveryagent.NewAgent(verifier, application)
	if err != nil {
		t.Fatal(err)
	}
	root := mustSignManagedResource(t, user, clusterName("guard"), 100, []byte(`{}`))
	if err := agent.Deliver(applicationPackage(root)); !errors.Is(err, deliveryagent.ErrFulfillmentRelationRequired) {
		t.Fatal(err)
	}
	if !application.mu.TryLock() {
		t.Fatal("evaluation failure leaked the application guard")
	}
	application.mu.Unlock()
	if len(application.pending) != 0 || application.accepts != 0 {
		t.Fatal("an early check admitted incompletely authorized work")
	}
}

func TestPrecheckFailureOrCancellationClosesAttemptBeforeAcceptance(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "precheck-error", true: "cancelled"}[cancel], func(t *testing.T) {
			verifier, user, _ := applicationVerifier(t)
			application := newQueuedApplication()
			application.guard = true
			ctx, cancelContext := context.WithCancel(context.Background())
			defer cancelContext()
			want := errors.New("progress query failed")
			if cancel {
				application.cancelOnPrecheck = cancelContext
				want = context.Canceled
			} else {
				application.precheckErr = want
			}
			agent, err := deliveryagent.NewAgent(verifier, application)
			if err != nil {
				t.Fatal(err)
			}
			root := mustSignDeployment(t, user, deploymentName("precheck-failure"), 1, []byte(`{}`))
			if err := agent.DeliverContext(ctx, applicationPackage(root)); !errors.Is(err, want) {
				t.Fatalf("precheck result: %v", err)
			}
			if application.accepts != 0 || application.closes != 1 || len(application.pending) != 0 {
				t.Fatal("failed precheck reached acceptance or leaked the attempt")
			}
			if !application.mu.TryLock() {
				t.Fatal("failed precheck retained its guard")
			}
			application.mu.Unlock()
		})
	}
}

func TestInvalidSignatureNeverReachesPrecheck(t *testing.T) {
	verifier, user, _ := applicationVerifier(t)
	root := mustSignDeployment(t, user, deploymentName("unverified"), 1, []byte(`{}`))
	root = tamperEmbeddedAssertion(t, root, func(assertion *protocol.TypedAssertion) {
		// Changing valid JSON preserves native hints but invalidates the real signature.
		assertion.Bytes = append(assertion.Bytes, ' ')
	})
	calls := 0
	_, err := verifier.Verify(context.Background(), applicationPackage(root), func(context.Context, protocol.DeliveryScope) (bool, error) {
		calls++
		return true, nil
	})
	if !errors.Is(err, protocol.ErrVerificationFailed) || calls != 0 {
		t.Fatalf("invalid signature: error=%v prechecks=%d", err, calls)
	}
}

func TestInvalidAuthenticatedScopeNeverReachesPrecheck(t *testing.T) {
	verifier, user, _ := applicationVerifier(t)
	root := mustSignDeploymentFor(t, user, "another-target", deploymentName("scope"), 1, []byte(`{}`))
	calls := 0
	_, err := verifier.Verify(context.Background(), applicationPackage(root), func(context.Context, protocol.DeliveryScope) (bool, error) {
		calls++
		return false, nil
	})
	if !errors.Is(err, protocol.ErrPolicyReevaluation) || calls != 0 {
		t.Fatalf("invalid scope: error=%v prechecks=%d", err, calls)
	}
}

// These fixtures compose the public verifier with real native signatures.
// No agent-owned application or private verification fields are used.
func applicationVerifier(t *testing.T) (*deliveryagent.Verifier, *producer.Producer, *producer.Producer) {
	t.Helper()
	target := directkey.NewTarget()
	verifier, err := deliveryagent.NewVerifier(deliveryagent.Config{Tenant: testTenantIdentity(), ProviderTenant: testTenantIdentity(), TargetID: testTarget}, func(kind protocol.ProvenanceType) (protocol.TargetAPI, bool) {
		return target, kind == target.ProvenanceType()
	})
	if err != nil {
		t.Fatal(err)
	}
	trust := testTrust()
	for i := range trust.AuthorityRegistry[0].DeliveryPolicies {
		trust.AuthorityRegistry[0].DeliveryPolicies[i].RequireEvidenceLog = false
	}
	if err := verifier.Bootstrap(trust); err != nil {
		t.Fatal(err)
	}
	user, addon := mustProducer(t, "application-user"), mustProducer(t, "application-addon")
	for _, signer := range []*producer.Producer{user, addon} {
		evidence, err := signer.DirectKey().CreateEnrollment()
		if err != nil {
			t.Fatal(err)
		}
		result, err := verifier.Verify(context.Background(), applicationPackage(evidence), nil)
		if err != nil {
			t.Fatal(err)
		}
		if result.SuiteEvent == nil || result.Action != nil || result.Completed {
			t.Fatal("enrollment did not yield a suite-owned event")
		}
		if _, exists := target.PublicKey(signer.Principal()); exists {
			t.Fatal("evaluation applied the suite event")
		}
		if err := result.SuiteEvent.Apply(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return verifier, user, addon
}

func applicationPackage(root protocol.TypedEvidence, supporting ...protocol.TypedEvidence) protocol.DeliveryPackage {
	pkg := protocol.DeliveryPackage{Root: protocol.Item{SignedStatement: protocol.SignedStatement{Evidence: root}}}
	for _, evidence := range supporting {
		pkg.Supporting = append(pkg.Supporting, protocol.Item{SignedStatement: protocol.SignedStatement{Evidence: evidence}})
	}
	return pkg
}

// Narrowing the attempt to its base interface deliberately omits Precheck.
// Deferred acceptance still uses the real memory application's generation fence.
type lateApplication struct {
	*deliveryagent.MemoryApplication
}

func (a lateApplication) NewAttempt() deliveryagent.ApplicationAttempt {
	return lateAttempt{a.MemoryApplication.NewAttempt()}
}

type lateAttempt struct {
	deliveryagent.ApplicationAttempt
}

// queuedApplication models a recoverable application queue without external I/O.
// Accepted actions survive attempt closure; finish executes from retained work.
type queuedApplication struct {
	mu               sync.Mutex
	guard            bool
	pending          map[protocol.FullResourceName]deliveryagent.AuthorizedAction
	effects          *deliveryagent.MemoryApplication
	failNext         error
	precheckErr      error
	cancelOnPrecheck context.CancelFunc
	accepts          int
	closes           int
}

func newQueuedApplication() *queuedApplication {
	return &queuedApplication{pending: make(map[protocol.FullResourceName]deliveryagent.AuthorizedAction), effects: deliveryagent.NewMemoryApplication()}
}

func (a *queuedApplication) NewAttempt() deliveryagent.ApplicationAttempt {
	return &queuedAttempt{application: a}
}

type queuedAttempt struct {
	application *queuedApplication
	locked      bool
}

func (a *queuedAttempt) Precheck(_ context.Context, _ protocol.DeliveryScope) (bool, error) {
	if a.application.guard {
		a.application.mu.Lock()
		a.locked = true
	}
	if a.application.cancelOnPrecheck != nil {
		a.application.cancelOnPrecheck()
	}
	// Accepted work remains in progress; it is never reported as completed.
	return false, a.application.precheckErr
}

func (a *queuedAttempt) Accept(_ context.Context, action deliveryagent.AuthorizedAction) error {
	queue := a.application
	if !a.locked {
		queue.mu.Lock()
		defer queue.mu.Unlock()
	}
	queue.accepts++
	if queue.failNext != nil {
		return queue.failNext
	}
	prior, exists := queue.pending[action.Scope.FullResourceName]
	if exists && prior.Scope.Generation > action.Scope.Generation {
		return deliveryagent.ErrGeneration
	}
	if !exists || prior.Scope.Generation < action.Scope.Generation {
		queue.pending[action.Scope.FullResourceName] = action.Clone()
	}
	return nil
}

func (a *queuedAttempt) Close() {
	if !a.locked {
		a.application.mu.Lock()
	}
	a.application.closes++
	a.application.mu.Unlock()
}

func (a *queuedApplication) finish(name protocol.FullResourceName) error {
	a.mu.Lock()
	action := a.pending[name].Clone()
	a.mu.Unlock()
	return a.finishAction(action)
}

func (a *queuedApplication) finishAction(action deliveryagent.AuthorizedAction) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	// Fence execution against accepted work, including work whose effects have
	// not begun. Applied-object annotations alone cannot provide this guarantee.
	if a.pending[action.Scope.FullResourceName].Scope.Generation > action.Scope.Generation {
		return deliveryagent.ErrGeneration
	}
	attempt := a.effects.NewAttempt()
	defer attempt.Close()
	return attempt.Accept(context.Background(), action)
}
