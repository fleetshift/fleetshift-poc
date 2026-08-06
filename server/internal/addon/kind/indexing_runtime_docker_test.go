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
)

// awaitDoneCtx waits for a delivery result until ctx is done.
func awaitDoneCtx(t *testing.T, ctx context.Context, ch <-chan domain.DeliveryResult) domain.DeliveryResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-ctx.Done():
		t.Fatal("timed out waiting for delivery result")
		return domain.DeliveryResult{}
	}
}

// TestKindAgent_Docker_IndexingRuntimeEnsureAndStop creates and removes a
// real kind cluster with a recording IndexingRuntime, asserting EnsureIndexer
// runs before Delivered and StopIndexer runs before destroy.
func TestKindAgent_Docker_IndexingRuntimeEnsureAndStop(t *testing.T) {
	checker := cluster.NewProvider()
	if _, err := checker.List(); err != nil {
		t.Skipf("Docker not available: %v", err)
	}

	const resourceID = "fleetshift-idx-lifecycle"
	kindName := encodedKindName(resourceID)
	t.Cleanup(func() {
		_ = checker.Delete(kindName, "")
		// Also remove a pre-ownership bare name left by older test runs.
		_ = checker.Delete(resourceID, "")
	})
	_ = checker.Delete(kindName, "")
	_ = checker.Delete(resourceID, "")

	runtime := &recordingIndexingRuntime{}
	reporter := newChannelReporter()
	agent := kindaddon.NewAgent(
		reporter,
		func(logger log.Logger) kindaddon.ClusterProvider {
			return cluster.NewProvider(cluster.ProviderWithLogger(logger))
		},
		kindaddon.WithIndexingRuntime(runtime),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	err := agent.Deliver(
		ctx,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID: "kind-idx", Type: kindaddon.TargetType, Name: "local-kind",
		}),
		"d-idx:kind-idx",
		[]domain.Manifest{{
			ManifestType: kindaddon.ClusterManifestType,
			Raw:          json.RawMessage(`{"name":"` + resourceID + `"}`),
		}},
		domain.DeliveryAuth{},
		nil,
		1,
	)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := awaitDoneCtx(t, ctx, reporter.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Fatalf("Deliver state = %q, want %q; message = %q",
			result.State, domain.DeliveryStateDelivered, result.Message)
	}
	if runtime.ensureCount() != 1 {
		t.Fatalf("EnsureIndexer calls = %d, want 1", runtime.ensureCount())
	}
	got := runtime.lastEnsure()
	if got.TargetID != domain.TargetID("k8s-"+resourceID) {
		t.Fatalf("Ensure TargetID = %q, want k8s-%s", got.TargetID, resourceID)
	}
	if len(got.Credential) == 0 || got.APIServer == "" {
		t.Fatalf("Ensure missing credential or api server: api=%q credLen=%d",
			got.APIServer, len(got.Credential))
	}

	for {
		clusters, err := checker.List()
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if slices.Contains(clusters, kindName) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for cluster %q", kindName)
		case <-time.After(2 * time.Second):
		}
	}

	removeReporter := newChannelReporter()
	removeAgent := kindaddon.NewAgent(
		removeReporter,
		func(logger log.Logger) kindaddon.ClusterProvider {
			return cluster.NewProvider(cluster.ProviderWithLogger(logger))
		},
		kindaddon.WithIndexingRuntime(runtime),
	)
	err = removeAgent.Remove(
		ctx,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}),
		"d-idx-rm:kind-idx",
		[]domain.Manifest{{
			ManifestType: kindaddon.ClusterManifestType,
			Raw:          json.RawMessage(`{"name":"` + resourceID + `"}`),
		}},
		domain.DeliveryAuth{},
		nil,
		2,
	)
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}

	removeResult := awaitDoneCtx(t, ctx, removeReporter.done)
	if removeResult.State != domain.DeliveryStateDelivered {
		t.Fatalf("Remove state = %q, want %q; message = %q",
			removeResult.State, domain.DeliveryStateDelivered, removeResult.Message)
	}
	stops := runtime.stopIDs()
	if len(stops) != 1 || stops[0] != domain.TargetID("k8s-"+resourceID) {
		t.Fatalf("StopIndexer calls = %v, want [k8s-%s]", stops, resourceID)
	}

	for {
		clusters, err := checker.List()
		if err != nil {
			t.Fatalf("List after remove: %v", err)
		}
		if !slices.Contains(clusters, kindName) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("cluster %q still present after Remove", kindName)
		case <-time.After(2 * time.Second):
		}
	}
}
