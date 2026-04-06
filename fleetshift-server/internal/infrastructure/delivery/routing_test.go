package delivery_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
)

type spyAgent struct {
	delivered []domain.DeliverInput
	removed   []domain.RemoveInput
}

func (s *spyAgent) Deliver(_ context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	s.delivered = append(s.delivered, domain.DeliverInput{
		Target:       target,
		DeliveryID:   deliveryID,
		DeploymentID: domain.DeploymentID(deliveryID),
		Manifests:    manifests,
	})
	return domain.DeliveryResult{State: domain.DeliveryStateDelivered}, nil
}

func (s *spyAgent) Remove(_ context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, _ *domain.DeliverySignaler) error {
	s.removed = append(s.removed, domain.RemoveInput{
		Target:       target,
		DeliveryID:   deliveryID,
		DeploymentID: domain.DeploymentID(deliveryID),
		Manifests:    manifests,
		Auth:         auth,
		Attestation:  att,
	})
	return nil
}

func TestRoutingDeliveryService_RoutesToCorrectAgent(t *testing.T) {
	router := delivery.NewRoutingDeliveryService()

	kindAgent := &spyAgent{}
	k8sAgent := &spyAgent{}
	router.Register("kind", kindAgent)
	router.Register("kubernetes", k8sAgent)

	ctx := context.Background()
	nop := &domain.DeliverySignaler{}
	kindTarget := domain.TargetInfo{ID: "k1", Type: "kind", Name: "local-kind"}
	k8sTarget := domain.TargetInfo{ID: "c1", Type: "kubernetes", Name: "prod-cluster"}

	manifests := []domain.Manifest{{Raw: json.RawMessage(`{}`)}}

	if _, err := router.Deliver(ctx, kindTarget, "d1:k1", manifests, domain.DeliveryAuth{}, nil, nop); err != nil {
		t.Fatalf("Deliver to kind: %v", err)
	}
	if _, err := router.Deliver(ctx, k8sTarget, "d2:c1", manifests, domain.DeliveryAuth{}, nil, nop); err != nil {
		t.Fatalf("Deliver to kubernetes: %v", err)
	}

	if len(kindAgent.delivered) != 1 {
		t.Fatalf("kindAgent: got %d deliveries, want 1", len(kindAgent.delivered))
	}
	if kindAgent.delivered[0].DeliveryID != "d1:k1" {
		t.Errorf("kindAgent: DeliveryID = %q, want %q", kindAgent.delivered[0].DeliveryID, "d1:k1")
	}

	if len(k8sAgent.delivered) != 1 {
		t.Fatalf("k8sAgent: got %d deliveries, want 1", len(k8sAgent.delivered))
	}
	if k8sAgent.delivered[0].DeliveryID != "d2:c1" {
		t.Errorf("k8sAgent: DeliveryID = %q, want %q", k8sAgent.delivered[0].DeliveryID, "d2:c1")
	}
}

func TestRoutingDeliveryService_RemoveRoutesToCorrectAgent(t *testing.T) {
	router := delivery.NewRoutingDeliveryService()

	agent := &spyAgent{}
	router.Register("kind", agent)

	ctx := context.Background()
	nop := &domain.DeliverySignaler{}
	target := domain.TargetInfo{ID: "k1", Type: "kind", Name: "local-kind"}

	if err := router.Remove(ctx, target, "d1:k1", nil, domain.DeliveryAuth{}, nil, nop); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	if len(agent.removed) != 1 {
		t.Fatalf("got %d removes, want 1", len(agent.removed))
	}
	if agent.removed[0].DeliveryID != "d1:k1" {
		t.Errorf("DeliveryID = %q, want %q", agent.removed[0].DeliveryID, "d1:k1")
	}
}

func TestRoutingDeliveryService_UnregisteredTypeReturnsError(t *testing.T) {
	router := delivery.NewRoutingDeliveryService()

	ctx := context.Background()
	nop := &domain.DeliverySignaler{}
	target := domain.TargetInfo{ID: "k1", Type: "unknown", Name: "target"}

	_, err := router.Deliver(ctx, target, "d1:k1", nil, domain.DeliveryAuth{}, nil, nop)
	if err == nil {
		t.Fatal("expected error for unregistered target type")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}

	err = router.Remove(ctx, target, "d1:k1", nil, domain.DeliveryAuth{}, nil, nop)
	if err == nil {
		t.Fatal("expected error for unregistered target type")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestRoutingDeliveryService_RegisterReplacesPrevious(t *testing.T) {
	router := delivery.NewRoutingDeliveryService()

	first := &spyAgent{}
	second := &spyAgent{}
	router.Register("kind", first)
	router.Register("kind", second)

	ctx := context.Background()
	nop := &domain.DeliverySignaler{}
	target := domain.TargetInfo{ID: "k1", Type: "kind", Name: "target"}
	manifests := []domain.Manifest{{Raw: json.RawMessage(`{}`)}}

	if _, err := router.Deliver(ctx, target, "d1:k1", manifests, domain.DeliveryAuth{}, nil, nop); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	if len(first.delivered) != 0 {
		t.Errorf("first agent received %d deliveries, want 0", len(first.delivered))
	}
	if len(second.delivered) != 1 {
		t.Errorf("second agent received %d deliveries, want 1", len(second.delivered))
	}
}
