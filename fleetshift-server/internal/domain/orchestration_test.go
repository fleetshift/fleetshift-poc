package domain_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// recordingRunner runs activities and records their names and target-related
// inputs in order so tests can assert execution sequence.
type recordingRunner struct {
	ctx     context.Context
	records []activityRecord
	delegate domain.DurableRunner
}

type activityRecord struct {
	Name string
	// TargetID is set for remove-from-target, generate-manifests, deliver-to-target.
	TargetID domain.TargetID
}

func (r *recordingRunner) ID() string { return r.delegate.ID() }
func (r *recordingRunner) Context() context.Context { return r.ctx }

func (r *recordingRunner) Run(activity domain.Activity[any, any], in any) (any, error) {
	name := activity.Name()
	var targetID domain.TargetID
	switch v := in.(type) {
	case domain.RemoveInput:
		targetID = v.Target.ID
	case domain.GenerateManifestsInput:
		targetID = v.Target.ID
	case domain.DeliverInput:
		targetID = v.Target.ID
	}
	r.records = append(r.records, activityRecord{Name: name, TargetID: targetID})
	return r.delegate.Run(activity, in)
}

// activityNames returns the ordered list of activity names recorded.
func (r *recordingRunner) activityNames() []string {
	names := make([]string, len(r.records))
	for i, rec := range r.records {
		names[i] = rec.Name
	}
	return names
}

// stubDeploymentRepo returns a fixed deployment for Get and accepts Update.
type stubDeploymentRepo struct {
	deployment domain.Deployment
	updated    *domain.Deployment
}

func (s *stubDeploymentRepo) Create(_ context.Context, d domain.Deployment) error {
	s.deployment = d
	return nil
}

func (s *stubDeploymentRepo) Get(_ context.Context, id domain.DeploymentID) (domain.Deployment, error) {
	if id != s.deployment.ID {
		return domain.Deployment{}, domain.ErrNotFound
	}
	return s.deployment, nil
}

func (s *stubDeploymentRepo) List(_ context.Context) ([]domain.Deployment, error) {
	return []domain.Deployment{s.deployment}, nil
}

func (s *stubDeploymentRepo) Update(_ context.Context, d domain.Deployment) error {
	s.updated = &d
	return nil
}

func (s *stubDeploymentRepo) Delete(_ context.Context, _ domain.DeploymentID) error { return nil }

// stubTargetRepo returns a fixed list for List.
type stubTargetRepo struct {
	targets []domain.TargetInfo
}

func (s *stubTargetRepo) Create(_ context.Context, t domain.TargetInfo) error {
	s.targets = append(s.targets, t)
	return nil
}

func (s *stubTargetRepo) Get(_ context.Context, id domain.TargetID) (domain.TargetInfo, error) {
	for _, t := range s.targets {
		if t.ID == id {
			return t, nil
		}
	}
	return domain.TargetInfo{}, domain.ErrNotFound
}

func (s *stubTargetRepo) List(_ context.Context) ([]domain.TargetInfo, error) {
	return s.targets, nil
}

func (s *stubTargetRepo) Delete(_ context.Context, _ domain.TargetID) error { return nil }

// noopDelivery implements DeliveryService with no-op Deliver and Remove.
type noopDelivery struct{}

func (noopDelivery) Deliver(_ context.Context, _ domain.TargetInfo, _ domain.DeploymentID, _ []domain.Manifest) (domain.DeliveryResult, error) {
	return domain.DeliveryResult{}, nil
}

func (noopDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeploymentID) error {
	return nil
}

func TestOrchestration_RemoveStepsRunBeforeDeliverSteps(t *testing.T) {
	// Deployment was previously on "old1"; placement now resolves to "new1", "new2".
	// Delta = Removed: [old1], Added: [new1, new2]. Plan = RemoveStep([old1]), DeliverStep([new1, new2]).
	// We assert that remove-from-target is invoked for old1 before generate-manifests for new1.
	deploymentID := domain.DeploymentID("d1")
	depRepo := &stubDeploymentRepo{
		deployment: domain.Deployment{
			ID:       deploymentID,
			ResolvedTargets: []domain.TargetID{"old1"},
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{"new1", "new2"},
			},
			RolloutStrategy: nil, // immediate
			State:           domain.DeploymentStatePending,
		},
	}
	targetRepo := &stubTargetRepo{
		targets: []domain.TargetInfo{
			{ID: "old1"},
			{ID: "new1"},
			{ID: "new2"},
		},
	}

	wf := &domain.OrchestrationWorkflow{
		Deployments: depRepo,
		Targets:     targetRepo,
		Delivery:    noopDelivery{},
		Strategies:  domain.DefaultStrategyFactory{},
	}
	ctx := context.Background()
	syncRunner := &syncRunnerImpl{ctx: ctx}
	recorder := &recordingRunner{ctx: ctx, delegate: syncRunner}

	_, err := wf.Run(recorder, deploymentID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Find indices of remove vs deliver/generate for the relevant targets.
	var removeOld1At, generateNew1At int
	removeOld1At = -1
	generateNew1At = -1
	for i, rec := range recorder.records {
		if rec.Name == "remove-from-target" && rec.TargetID == "old1" {
			removeOld1At = i
			break
		}
	}
	for i, rec := range recorder.records {
		if rec.Name == "generate-manifests" && rec.TargetID == "new1" {
			generateNew1At = i
			break
		}
	}
	if removeOld1At < 0 {
		t.Fatal("remove-from-target for old1 never recorded")
	}
	if generateNew1At < 0 {
		t.Fatal("generate-manifests for new1 never recorded")
	}
	if removeOld1At >= generateNew1At {
		t.Errorf("removals must run before delivery: remove(old1) at %d, generate(new1) at %d",
			removeOld1At, generateNew1At)
	}
}

// TestOrchestration_PlacementAndRolloutRunAsActivities ensures placement
// resolution and rollout planning are invoked as activities, not inline
// in the workflow, so strategies may perform I/O or stateful behavior.
func TestOrchestration_PlacementAndRolloutRunAsActivities(t *testing.T) {
	deploymentID := domain.DeploymentID("d1")
	depRepo := &stubDeploymentRepo{
		deployment: domain.Deployment{
			ID:                deploymentID,
			ResolvedTargets:   nil,
			ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
			PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
			RolloutStrategy:   nil,
			State:             domain.DeploymentStatePending,
		},
	}
	targetRepo := &stubTargetRepo{targets: []domain.TargetInfo{{ID: "t1"}}}

	wf := &domain.OrchestrationWorkflow{
		Deployments: depRepo,
		Targets:     targetRepo,
		Delivery:    noopDelivery{},
		Strategies:  domain.DefaultStrategyFactory{},
	}
	ctx := context.Background()
	syncRunner := &syncRunnerImpl{ctx: ctx}
	recorder := &recordingRunner{ctx: ctx, delegate: syncRunner}

	_, err := wf.Run(recorder, deploymentID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	names := recorder.activityNames()
	hasResolvePlacement := false
	hasPlanRollout := false
	for _, n := range names {
		if n == "resolve-placement" {
			hasResolvePlacement = true
		}
		if n == "plan-rollout" {
			hasPlanRollout = true
		}
	}
	if !hasResolvePlacement {
		t.Errorf("workflow must invoke resolve-placement activity; got names: %v", names)
	}
	if !hasPlanRollout {
		t.Errorf("workflow must invoke plan-rollout activity; got names: %v", names)
	}
}

// syncRunnerImpl runs activities synchronously (no durability).
type syncRunnerImpl struct {
	ctx context.Context
}

func (s *syncRunnerImpl) ID() string { return "test-sync" }
func (s *syncRunnerImpl) Context() context.Context { return s.ctx }
func (s *syncRunnerImpl) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(s.ctx, in)
}
