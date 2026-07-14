package kubernetes_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// recordingInventoryReportBackend records every InventoryReportBackend
// call so DirectInventoryReporter mapping can be asserted without a
// real InventoryReportService, informer, or store.
type recordingInventoryReportBackend struct {
	replaceBatchCalls []replaceBatchCall
	replaceBatchErr   error
}

type replaceBatchCall struct {
	resourceType domain.ResourceType
	reports      []kubernetes.InventoryObjectReport
}

func (r *recordingInventoryReportBackend) ReplaceBatch(_ context.Context, resourceType domain.ResourceType, reports []kubernetes.InventoryObjectReport) error {
	r.replaceBatchCalls = append(r.replaceBatchCalls, replaceBatchCall{resourceType: resourceType, reports: reports})
	return r.replaceBatchErr
}

func TestDirectInventoryReporter_ApplyDelta_EmptyIsNoop(t *testing.T) {
	fake := &recordingInventoryReportBackend{}
	reporter := kubernetes.NewDirectInventoryReporter(fake)

	if err := reporter.ApplyDelta(context.Background(), kubernetes.InventoryDeltaReport{}); err != nil {
		t.Fatalf("ApplyDelta(empty): %v", err)
	}
	if len(fake.replaceBatchCalls) != 0 {
		t.Fatalf("empty ApplyDelta must not call backend; got replace=%d", len(fake.replaceBatchCalls))
	}
}

func TestDirectInventoryReporter_ApplyDelta_MapsUpsertsAndDeletes(t *testing.T) {
	fake := &recordingInventoryReportBackend{}
	reporter := kubernetes.NewDirectInventoryReporter(fake)
	now := time.Unix(1700000000, 0).UTC()
	obs := json.RawMessage(`{"kind":"Pod"}`)
	name := domain.ResourceName("clusters/prod/apiResources/core~v1~pods/objects/uid-1")
	delName := domain.ResourceName("clusters/prod/apiResources/core~v1~pods/objects/uid-gone")

	err := reporter.ApplyDelta(context.Background(), kubernetes.InventoryDeltaReport{
		Upserts: []kubernetes.InventoryObjectReport{{
			Name:        name,
			Labels:      map[string]string{"k8s.uid": "uid-1"},
			Observation: &obs,
			ObservedAt:  now,
		}},
		Deletes: []kubernetes.InventoryObjectReport{{
			Name:     delName,
			IsDelete: true,
		}},
	})
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}

	if len(fake.replaceBatchCalls) != 1 {
		t.Fatalf("ReplaceBatch calls = %d, want 1", len(fake.replaceBatchCalls))
	}
	gotBatch := fake.replaceBatchCalls[0]
	if gotBatch.resourceType != kubernetes.ObjectResourceType {
		t.Errorf("ReplaceBatch resourceType = %q, want %q", gotBatch.resourceType, kubernetes.ObjectResourceType)
	}
	if len(gotBatch.reports) != 2 {
		t.Fatalf("ReplaceBatch reports = %d, want 2 (upsert + delete)", len(gotBatch.reports))
	}
	if gotBatch.reports[0].Name != name || gotBatch.reports[0].IsDelete {
		t.Errorf("ReplaceBatch upsert = %+v, want name %q IsDelete=false", gotBatch.reports[0], name)
	}
	if !reflect.DeepEqual(gotBatch.reports[0].Labels, map[string]string{"k8s.uid": "uid-1"}) {
		t.Errorf("ReplaceBatch labels = %#v", gotBatch.reports[0].Labels)
	}
	if gotBatch.reports[0].Observation == nil || string(*gotBatch.reports[0].Observation) != string(obs) {
		t.Errorf("ReplaceBatch observation = %v, want %s", gotBatch.reports[0].Observation, obs)
	}
	if !gotBatch.reports[0].ObservedAt.Equal(now) {
		t.Errorf("ReplaceBatch ObservedAt = %v, want %v", gotBatch.reports[0].ObservedAt, now)
	}
	if gotBatch.reports[1].Name != delName || !gotBatch.reports[1].IsDelete {
		t.Errorf("ReplaceBatch delete = %+v, want name %q IsDelete=true", gotBatch.reports[1], delName)
	}
}

func TestDirectInventoryReporter_ApplyDelta_UpsertsOnly(t *testing.T) {
	fake := &recordingInventoryReportBackend{}
	reporter := kubernetes.NewDirectInventoryReporter(fake)
	name := domain.ResourceName("clusters/prod/apiResources/apps~v1~deployments/objects/uid-d1")

	err := reporter.ApplyDelta(context.Background(), kubernetes.InventoryDeltaReport{
		Upserts: []kubernetes.InventoryObjectReport{{Name: name}},
	})
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	if len(fake.replaceBatchCalls) != 1 {
		t.Fatalf("ReplaceBatch calls = %d, want 1", len(fake.replaceBatchCalls))
	}
}

func TestDirectInventoryReporter_ApplyDelta_DeletesOnly(t *testing.T) {
	fake := &recordingInventoryReportBackend{}
	reporter := kubernetes.NewDirectInventoryReporter(fake)
	delName := domain.ResourceName("clusters/prod/apiResources/core~v1~pods/objects/uid-x")

	err := reporter.ApplyDelta(context.Background(), kubernetes.InventoryDeltaReport{
		Deletes: []kubernetes.InventoryObjectReport{{
			Name:     delName,
			IsDelete: true,
		}},
	})
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	if len(fake.replaceBatchCalls) != 1 {
		t.Fatalf("ReplaceBatch calls = %d, want 1", len(fake.replaceBatchCalls))
	}
	got := fake.replaceBatchCalls[0].reports
	if len(got) != 1 || got[0].Name != delName || !got[0].IsDelete {
		t.Fatalf("ReplaceBatch reports = %#v, want IsDelete for %q", got, delName)
	}
}

func TestDirectInventoryReporter_ApplyDelta_ForcesIsDeleteOnDeletes(t *testing.T) {
	// Writer should set IsDelete, but the reporter must still force it
	// so a mis-tagged delete entry cannot become an upsert.
	fake := &recordingInventoryReportBackend{}
	reporter := kubernetes.NewDirectInventoryReporter(fake)
	delName := domain.ResourceName("clusters/prod/apiResources/core~v1~pods/objects/uid-x")

	err := reporter.ApplyDelta(context.Background(), kubernetes.InventoryDeltaReport{
		Deletes: []kubernetes.InventoryObjectReport{{Name: delName}},
	})
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	got := fake.replaceBatchCalls[0].reports
	if len(got) != 1 || !got[0].IsDelete {
		t.Fatalf("ReplaceBatch reports = %#v, want forced IsDelete=true", got)
	}
}

func TestDirectInventoryReporter_ApplyDelta_PropagatesReplaceBatchError(t *testing.T) {
	wantErr := errors.New("replace failed")
	fake := &recordingInventoryReportBackend{replaceBatchErr: wantErr}
	reporter := kubernetes.NewDirectInventoryReporter(fake)

	err := reporter.ApplyDelta(context.Background(), kubernetes.InventoryDeltaReport{
		Upserts: []kubernetes.InventoryObjectReport{{Name: "clusters/prod/apiResources/core~v1~pods/objects/uid-1"}},
		Deletes: []kubernetes.InventoryObjectReport{{
			Name:     "clusters/prod/apiResources/core~v1~pods/objects/uid-2",
			IsDelete: true,
		}},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ApplyDelta error = %v, want wrapped %v", err, wantErr)
	}
}

func TestInventoryReporter_DTOsHaveNoEdgeFields(t *testing.T) {
	// Inventory reporter DTOs must not carry edge fields; edge output
	// is isolated behind EdgeSink. InventoryDeltaReport.Deletes is
	// object reports with IsDelete, not topology edges.
	for _, typ := range []any{
		kubernetes.InventoryDeltaReport{},
		kubernetes.InventoryObjectReport{},
	} {
		rt := reflect.TypeOf(typ)
		for i := 0; i < rt.NumField(); i++ {
			name := rt.Field(i).Name
			switch name {
			case "Adds", "Deletes", "Edges", "EdgeAdds", "EdgeDeletes", "EdgeDelta":
				if rt == reflect.TypeOf(kubernetes.InventoryDeltaReport{}) && name == "Deletes" {
					continue
				}
				t.Errorf("%s has edge-related field %q", rt.Name(), name)
			}
		}
	}
}
