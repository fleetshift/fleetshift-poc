package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres"
)

func TestResourceRepresentationsSchema_DoesNotExposeLegacyDeletedAt(t *testing.T) {
	t.Parallel()

	db := postgres.OpenTestDB(t)

	var exists bool
	if err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = 'resource_representations'
			  AND column_name = 'deleted_at'
		)`).
		Scan(&exists); err != nil {
		t.Fatalf("query information_schema.columns: %v", err)
	}
	if exists {
		t.Fatal("resource_representations.deleted_at should have been removed")
	}
}

func TestExtensionResourcesReportedAliases_StoredAsObjectPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := postgres.OpenTestDB(t)
	repo := postgres.ExtensionResourceRepo{DB: db}

	rt := domain.ResourceType("shape.fleetshift.io/Node")
	if err := repo.CreateType(ctx, domain.NewExtensionResourceType(
		rt, "v1", "nodes", time.Unix(0, 0).UTC(), domain.WithInventory(),
	)); err != nil {
		t.Fatalf("CreateType: %v", err)
	}

	alias, err := domain.NewAlias("gcp", "instance_id", "shape-instance")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	if err := repo.ApplyInventoryDeltas(ctx, []domain.InventoryDelta{{
		ResourceType:  rt,
		Name:          "nodes/shape",
		CandidateUID:  domain.NewExtensionResourceUID(),
		UpsertAliases: domain.NewAliasSet([]domain.Alias{alias}),
		ObservedAt:    now,
		ReceivedAt:    now,
	}}); err != nil {
		t.Fatalf("ApplyInventoryDeltas: %v", err)
	}

	var payloadType, payloadText string
	if err := db.QueryRowContext(ctx, `
		SELECT jsonb_typeof(reported_aliases), reported_aliases::text
		FROM extension_resources
		WHERE service_name = $1 AND collection_name = $2 AND resource_id = $3`,
		string(rt.ServiceName()), "nodes", "shape",
	).Scan(&payloadType, &payloadText); err != nil {
		t.Fatalf("read reported_aliases: %v", err)
	}
	if payloadType != "object" {
		t.Fatalf("jsonb_typeof(reported_aliases) = %q, want object; payload=%s", payloadType, payloadText)
	}

	var payload map[string]string
	if err := json.Unmarshal([]byte(payloadText), &payload); err != nil {
		t.Fatalf("unmarshal raw payload: %v", err)
	}
	encodedKey, err := json.Marshal([2]string{"gcp", "instance_id"})
	if err != nil {
		t.Fatalf("marshal expected key: %v", err)
	}
	if got := payload[string(encodedKey)]; got != "shape-instance" {
		t.Fatalf("payload[%s] = %q, want %q; payload=%s", encodedKey, got, "shape-instance", payloadText)
	}
}
