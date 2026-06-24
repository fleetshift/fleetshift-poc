//go:build integration

package kind_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/log"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/testharness"
)

// TestKindAddon_RealDocker exercises the full addon lifecycle against
// a real Docker daemon. It creates a kind cluster, verifies it exists,
// then tears it down. Skipped when Docker is not available.
// Requires -tags integration.
func TestKindAddon_RealDocker(t *testing.T) {
	checker := cluster.NewProvider()

	if _, err := checker.List(); err != nil {
		t.Skipf("Docker not available: %v", err)
	}

	const clusterName = "fleetshift-test"

	t.Cleanup(func() {
		_ = checker.Delete(clusterName, "")
	})
	// Pre-clean in case a previous run left a stale cluster.
	_ = checker.Delete(clusterName, "")

	h := testharness.New(t)

	kindAgent := kindaddon.NewAgent(h.Reporter, func(logger log.Logger) kindaddon.ClusterProvider {
		return cluster.NewProvider(cluster.ProviderWithLogger(logger))
	})
	h.Router.Register(kindaddon.TargetType, kindAgent)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := h.Targets.Register(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "kind-docker",
		Type: kindaddon.TargetType,
		Name: "Docker Kind Provider",
	})); err != nil {
		t.Fatalf("Register target: %v", err)
	}

	spec := kindaddon.ClusterSpec{Name: clusterName}
	specBytes, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}

	_, err = h.Deployments.Create(ctx, domain.CreateDeploymentInput{
		ID: "kind-docker-deploy",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{
				ResourceType: kindaddon.ClusterResourceType,
				Raw:          json.RawMessage(specBytes),
			}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"kind-docker"},
		},
	})
	if err != nil {
		t.Fatalf("Create deployment: %v", err)
	}

	view := awaitState(ctx, t, h.Store, "kind-docker-deploy", domain.FulfillmentStateActive)
	if len(view.Fulfillment.ResolvedTargets()) != 1 || view.Fulfillment.ResolvedTargets()[0] != "kind-docker" {
		t.Fatalf("unexpected ResolvedTargets: %v", view.Fulfillment.ResolvedTargets())
	}

	// Delivery is async; poll until the cluster appears or context expires.
	for {
		clusters, err := checker.List()
		if err != nil {
			t.Fatalf("provider.List: %v", err)
		}
		if slices.Contains(clusters, clusterName) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for kind cluster %q to be created", clusterName)
		case <-time.After(5 * time.Second):
		}
	}
}
