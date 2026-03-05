package kubernetes_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type stubVault struct {
	secrets map[domain.SecretRef][]byte
}

func (v *stubVault) Put(_ context.Context, ref domain.SecretRef, value []byte) error {
	v.secrets[ref] = value
	return nil
}

func (v *stubVault) Get(_ context.Context, ref domain.SecretRef) ([]byte, error) {
	val, ok := v.secrets[ref]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return val, nil
}

func (v *stubVault) Delete(_ context.Context, ref domain.SecretRef) error {
	delete(v.secrets, ref)
	return nil
}

// channelDeliveryObserver collects events and completion results on
// channels, enabling deterministic waits in tests with async delivery.
type channelDeliveryObserver struct {
	mu     sync.Mutex
	events []domain.DeliveryEvent
	ch     chan domain.DeliveryEvent
	done   chan domain.DeliveryResult
}

func newChannelDeliveryObserver() *channelDeliveryObserver {
	return &channelDeliveryObserver{
		ch:   make(chan domain.DeliveryEvent, 100),
		done: make(chan domain.DeliveryResult, 1),
	}
}

func (o *channelDeliveryObserver) EventEmitted(ctx context.Context, _ domain.DeliveryID, _ domain.TargetInfo, e domain.DeliveryEvent) (context.Context, domain.EventEmittedProbe) {
	o.mu.Lock()
	o.events = append(o.events, e)
	o.mu.Unlock()
	o.ch <- e
	return ctx, domain.NoOpEventEmittedProbe{}
}

func (o *channelDeliveryObserver) Completed(ctx context.Context, _ domain.DeliveryID, _ domain.TargetInfo, result domain.DeliveryResult) (context.Context, domain.CompletedProbe) {
	o.done <- result
	return ctx, domain.NoOpCompletedProbe{}
}

func newChannelSignaler(obs *channelDeliveryObserver) *domain.DeliverySignaler {
	return domain.NewDeliverySignaler("", "", domain.TargetInfo{}, nil, nil, obs)
}

func TestAgent_Deliver_MissingKubeconfigRef(t *testing.T) {
	vault := &stubVault{secrets: make(map[domain.SecretRef][]byte)}
	agent := kubernetes.NewAgent(vault)

	target := domain.TargetInfo{
		ID:         "k8s-test",
		Type:       kubernetes.TargetType,
		Name:       "test-cluster",
		Properties: map[string]string{},
	}

	result, err := agent.Deliver(context.Background(), target, "d1", nil, &domain.DeliverySignaler{})
	if err == nil {
		t.Fatal("expected error for missing kubeconfig_ref")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateFailed)
	}
}

func TestAgent_Deliver_VaultNotFound(t *testing.T) {
	vault := &stubVault{secrets: make(map[domain.SecretRef][]byte)}
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)
	agent := kubernetes.NewAgent(vault)

	target := domain.TargetInfo{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"kubeconfig_ref": "targets/k8s-test/kubeconfig",
		},
	}

	result, err := agent.Deliver(context.Background(), target, "d1", nil, signaler)
	if err != nil {
		t.Fatalf("Deliver should not return error after ack: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	asyncResult := <-obs.done
	if asyncResult.State != domain.DeliveryStateFailed {
		t.Errorf("async State = %q, want %q", asyncResult.State, domain.DeliveryStateFailed)
	}
}

func TestAgent_Deliver_InvalidKubeconfig(t *testing.T) {
	vault := &stubVault{secrets: map[domain.SecretRef][]byte{
		"targets/k8s-test/kubeconfig": []byte("not a valid kubeconfig"),
	}}
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)
	agent := kubernetes.NewAgent(vault)

	target := domain.TargetInfo{
		ID:   "k8s-test",
		Type: kubernetes.TargetType,
		Name: "test-cluster",
		Properties: map[string]string{
			"kubeconfig_ref": "targets/k8s-test/kubeconfig",
		},
	}

	manifests := []domain.Manifest{{
		ResourceType: "raw",
		Raw:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test","namespace":"default"},"data":{"key":"value"}}`),
	}}

	result, err := agent.Deliver(context.Background(), target, "d1", manifests, signaler)
	if err != nil {
		t.Fatalf("Deliver should not return error after ack: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	asyncResult := <-obs.done
	if asyncResult.State != domain.DeliveryStateFailed {
		t.Errorf("async State = %q, want %q", asyncResult.State, domain.DeliveryStateFailed)
	}
}

func TestAgent_Remove_IsNoop(t *testing.T) {
	vault := &stubVault{secrets: make(map[domain.SecretRef][]byte)}
	agent := kubernetes.NewAgent(vault)

	target := domain.TargetInfo{ID: "k8s-test", Type: kubernetes.TargetType, Name: "test-cluster"}
	if err := agent.Remove(context.Background(), target, "d1", &domain.DeliverySignaler{}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
}
