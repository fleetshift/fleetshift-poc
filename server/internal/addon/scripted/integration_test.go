package scripted_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/scripted"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// integrationHarness bundles the full application stack needed to drive
// a scripted managed resource through the orchestration workflow.
type integrationHarness struct {
	store       domain.Store
	resourceSvc *application.ExtensionResourceService
}

// setupIntegration constructs a complete in-memory application stack
// with the scripted delivery agent wired through the routing service,
// orchestration workflow, and extension resource service.
//
// opts are forwarded to the OrchestrationWorkflowSpec (e.g.
// WithAckRetryInterval for faster retry tests).
func setupIntegration(t *testing.T, opts ...domain.OrchestrationWorkflowOption) integrationHarness {
	t.Helper()

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	reg := &memworkflow.Registry{}
	deliveryReporter := application.NewDeliveryReportService(store, reg)
	inventorySvc := application.NewInventoryReportService(store)
	inventoryReporter := application.NewInventoryReporterAdapter(inventorySvc)

	appCtx, appCancel := context.WithCancel(context.Background())
	t.Cleanup(appCancel)

	codec, err := scripted.NewCodec(context.Background())
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}
	agent := scripted.NewAgent(deliveryReporter, inventoryReporter, codec, scripted.NewPlanner(), appCtx)
	t.Cleanup(func() { _ = agent.Close(context.Background()) })

	router := delivery.NewRoutingDeliveryService()
	router.Register(scripted.TargetType, agent)

	orchWf, err := reg.RegisterOrchestration(domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg, opts...,
	))
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	createMRWf, err := reg.RegisterCreateManagedResource(&domain.CreateManagedResourceWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	})
	if err != nil {
		t.Fatalf("RegisterCreateManagedResource: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	// Register target.
	{
		tx, err := store.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if err := tx.Targets().Create(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
			ID:                    scripted.TargetID,
			Type:                  scripted.TargetType,
			Name:                  "Local Scripted Provider",
			AcceptedManifestTypes: []domain.ManifestType{scripted.ManagedManifestType},
		})); err != nil {
			t.Fatalf("Create target: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit target: %v", err)
		}
	}

	// Register extension resource type.
	typeSvc := application.NewExtensionResourceTypeService(store)
	if _, err := typeSvc.Create(ctx, application.CreateExtensionTypeInput{
		ResourceType: scripted.ResourceType,
		APIVersion:   "v1",
		CollectionID: scripted.CollectionID,
		Management: &application.CreateExtensionTypeManagementInput{
			Relation: domain.NewRegisteredSelfTarget(scripted.TargetID, scripted.ManagedManifestType),
			Signature: domain.Signature{
				Signer:         domain.FederatedIdentity{Subject: "scripted-addon", Issuer: "https://scripted.test"},
				ContentHash:    []byte("hash"),
				SignatureBytes: []byte("sig"),
			},
		},
	}); err != nil {
		t.Fatalf("RegisterType: %v", err)
	}

	return integrationHarness{
		store:       store,
		resourceSvc: application.NewExtensionResourceService(store, createMRWf, nil, nil, nil),
	}
}

// TestScriptedAddon_ManagedResource_EndToEnd exercises the full managed
// resource path through the scripted delivery agent:
//
//  1. Register the scripted delivery agent with the routing service.
//  2. Register a target that accepts scripted managed resources.
//  3. Register the scripted managed resource type.
//  4. Create a managed resource with a scripted spec via the service.
//  5. Verify the fulfillment reaches Active state.
func TestScriptedAddon_ManagedResource_EndToEnd(t *testing.T) {
	h := setupIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	spec := json.RawMessage(`{
		"behavior": {
			"delivery": {
				"acknowledgement": {
					"outcome": {"constant": "SUCCESS"}
				},
				"completion": {
					"outcome": {"constant": "SUCCESS"}
				}
			}
		},
		"inventory": {
			"labels": {"test-key": "test-value"},
			"observation": {"status": "ok"}
		}
	}`)

	view, err := h.resourceSvc.Create(ctx, application.CreateExtensionResourceInput{
		ResourceType: scripted.ResourceType,
		Name:         scripted.CollectionID + "/test-scripted-resource",
		Spec:         spec,
	})
	if err != nil {
		t.Fatalf("Create extension resource: %v", err)
	}

	awaitFulfillment(ctx, t, h.store, view.Fulfillment.ID(), domain.FulfillmentStateActive)
}

// TestScriptedAddon_ManagedResource_AckRetry exercises the retry path:
// the scripted agent fails acknowledgement on the first attempt, the
// platform retries, and the second attempt succeeds.
func TestScriptedAddon_ManagedResource_AckRetry(t *testing.T) {
	h := setupIntegration(t,
		// Short retry interval so the test doesn't wait long.
		domain.WithAckRetryInterval(50*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	spec := json.RawMessage(`{
		"behavior": {
			"delivery": {
				"acknowledgement": {
					"outcome": {"sequence": {"values": ["FAILURE", "FAILURE", "SUCCESS"]}}
				}
			}
		}
	}`)

	view, err := h.resourceSvc.Create(ctx, application.CreateExtensionResourceInput{
		ResourceType: scripted.ResourceType,
		Name:         scripted.CollectionID + "/retry-test",
		Spec:         spec,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// The orchestration should retry after the ack failures and
	// eventually reach Active.
	awaitFulfillment(ctx, t, h.store, view.Fulfillment.ID(), domain.FulfillmentStateActive)
}

// awaitFulfillment polls the store until the fulfillment reaches the
// desired state. It fails immediately if the fulfillment reaches an
// unexpected terminal state (failed) that can never transition to the
// wanted state.
func awaitFulfillment(ctx context.Context, t *testing.T, store domain.Store, fID domain.FulfillmentID, want domain.FulfillmentState) {
	t.Helper()
	for {
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		f, err := tx.Fulfillments().Get(ctx, fID)
		tx.Rollback()

		if err == nil {
			got := f.State()
			if got == want {
				return
			}
			// Fail fast when the fulfillment reaches a terminal state
			// that cannot transition to the wanted state.
			if got == domain.FulfillmentStateFailed && want != domain.FulfillmentStateFailed {
				t.Fatalf("fulfillment %s reached terminal state %q, wanted %q", fID, got, want)
			}
		}

		select {
		case <-ctx.Done():
			var state domain.FulfillmentState
			if err == nil && f != nil {
				state = f.State()
			}
			t.Fatalf("timed out waiting for fulfillment %s to reach state %q (current: %q)", fID, want, state)
		case <-time.After(5 * time.Millisecond):
		}
	}
}
