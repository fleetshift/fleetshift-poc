package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func TestResourceRepresentationsSchema_DoesNotExposeLegacyDeletedAt(t *testing.T) {
	db := sqlite.OpenTestDB(t)

	rows, err := db.Query(`PRAGMA table_info(resource_representations)`)
	if err != nil {
		t.Fatalf("PRAGMA table_info(resource_representations): %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			name       string
			columnType string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultVal, &pk); err != nil {
			t.Fatalf("scan table_info row: %v", err)
		}
		if name == "deleted_at" {
			t.Fatal("resource_representations.deleted_at should have been removed")
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table_info rows: %v", err)
	}
}

func TestExtensionResourcesReportedAliases_StoredAsObjectPayload(t *testing.T) {
	ctx := context.Background()
	db := sqlite.OpenTestDB(t)
	repo := sqlite.ExtensionResourceRepo{DB: db}

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

	var payloadText string
	if err := db.QueryRowContext(ctx, `
		SELECT reported_aliases
		FROM extension_resources
		WHERE service_name = ? AND collection_name = ? AND resource_id = ?`,
		string(rt.ServiceName()), "nodes", "shape",
	).Scan(&payloadText); err != nil {
		t.Fatalf("read reported_aliases: %v", err)
	}

	var payload map[string]string
	if err := json.Unmarshal([]byte(payloadText), &payload); err != nil {
		t.Fatalf("unmarshal raw payload as object: %v (payload=%s)", err, payloadText)
	}
	encodedKey, err := json.Marshal([2]string{"gcp", "instance_id"})
	if err != nil {
		t.Fatalf("marshal expected key: %v", err)
	}
	if got := payload[string(encodedKey)]; got != "shape-instance" {
		t.Fatalf("payload[%s] = %q, want %q; payload=%s", encodedKey, got, "shape-instance", payloadText)
	}
}
