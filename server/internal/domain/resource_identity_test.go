package domain

import (
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"
)

func collectAliasSet(set AliasSet) []Alias {
	return set.Slice()
}

// ---------------------------------------------------------------------------
// Value object constructors
// ---------------------------------------------------------------------------

func TestNewServiceName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "valid", input: "kind.fleetshift.io"},
		{name: "empty rejected", input: "", wantErr: true},
		{name: "slash rejected", input: "a/b", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewServiceName(tt.input)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidArgument) {
					t.Errorf("got %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != ServiceName(tt.input) {
				t.Errorf("got %q, want %q", got, tt.input)
			}
		})
	}
}

func TestNewAPIVersion(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "valid", input: "v1alpha1"},
		{name: "empty rejected", input: "", wantErr: true},
		{name: "no v prefix rejected", input: "1alpha1", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewAPIVersion(tt.input)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidArgument) {
					t.Errorf("got %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != APIVersion(tt.input) {
				t.Errorf("got %q, want %q", got, tt.input)
			}
		})
	}
}

func TestNewCollectionID(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "valid lowercase", input: "clusters"},
		{name: "valid camelCase", input: "userEvents"},
		{name: "empty rejected", input: "", wantErr: true},
		{name: "UpperCamelCase rejected", input: "Clusters", wantErr: true},
		{name: "slash rejected", input: "a/b", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewCollectionID(tt.input)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidArgument) {
					t.Errorf("got %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != CollectionID(tt.input) {
				t.Errorf("got %q, want %q", got, tt.input)
			}
		})
	}
}

func TestNewResourceID(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "valid", input: "prod-us-east-1"},
		{name: "empty rejected", input: "", wantErr: true},
		{name: "slash rejected", input: "a/b", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewResourceID(tt.input)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidArgument) {
					t.Errorf("got %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != ResourceID(tt.input) {
				t.Errorf("got %q, want %q", got, tt.input)
			}
		})
	}
}

func TestNewAlias(t *testing.T) {
	tests := []struct {
		name    string
		ns      AliasNamespace
		key     AliasKey
		value   AliasValue
		wantErr bool
	}{
		{name: "valid", ns: "gcp", key: "project_id", value: "my-proj"},
		{name: "empty namespace rejected", ns: "", key: "k", value: "v", wantErr: true},
		{name: "empty key rejected", ns: "gcp", key: "", value: "v", wantErr: true},
		{name: "empty value rejected", ns: "gcp", key: "k", value: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewAlias(tt.ns, tt.key, tt.value)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidArgument) {
					t.Errorf("got %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Namespace() != tt.ns || got.Key() != tt.key || got.Value() != tt.value {
				t.Errorf("got %+v, want ns=%q key=%q value=%q", got, tt.ns, tt.key, tt.value)
			}
		})
	}
}

func TestAliasSet_CanonicalizesSortsAndMergesByRef(t *testing.T) {
	projectV1, err := NewAlias("gcp", "project_id", "proj-v1")
	if err != nil {
		t.Fatalf("NewAlias(projectV1): %v", err)
	}
	projectV2, err := NewAlias("gcp", "project_id", "proj-v2")
	if err != nil {
		t.Fatalf("NewAlias(projectV2): %v", err)
	}
	zone, err := NewAlias("gcp", "zone", "us-central1-a")
	if err != nil {
		t.Fatalf("NewAlias(zone): %v", err)
	}
	account, err := NewAlias("aws", "account_id", "123456789012")
	if err != nil {
		t.Fatalf("NewAlias(account): %v", err)
	}

	set := NewAliasSet([]Alias{zone, projectV1, account, projectV2})

	if got := set.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}

	var got []Alias
	for alias := range set.All() {
		got = append(got, alias)
	}
	want := []Alias{account, projectV2, zone}
	if len(got) != len(want) {
		t.Fatalf("All() len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("All()[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestAliasSet_Slice_ReturnsCanonicalCopy(t *testing.T) {
	project, err := NewAlias("gcp", "project_id", "proj-1")
	if err != nil {
		t.Fatalf("NewAlias(project): %v", err)
	}
	zone, err := NewAlias("gcp", "zone", "us-central1-a")
	if err != nil {
		t.Fatalf("NewAlias(zone): %v", err)
	}

	set := NewAliasSet([]Alias{zone, project})
	got := set.Slice()
	want := []Alias{project, zone}
	if !slices.Equal(got, want) {
		t.Fatalf("Slice() = %+v, want %+v", got, want)
	}

	got[0] = zone
	if again := set.Slice(); !slices.Equal(again, want) {
		t.Fatalf("Slice() after mutating returned slice = %+v, want %+v", again, want)
	}
}

func TestAlias_JSONRoundTrip_UsesSnapshotShape(t *testing.T) {
	alias, err := NewAlias("gcp", "project_id", "my-proj")
	if err != nil {
		t.Fatalf("NewAlias: %v", err)
	}

	data, err := json.Marshal(alias)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if got, want := string(data), `{"namespace":"gcp","key":"project_id","value":"my-proj"}`; got != want {
		t.Fatalf("MarshalJSON() = %s, want %s", got, want)
	}

	var roundTripped Alias
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if roundTripped != alias {
		t.Fatalf("roundTripped = %+v, want %+v", roundTripped, alias)
	}
}

func TestAliasSet_JSONRoundTrip_EmptyUsesArrayLiteral(t *testing.T) {
	var set AliasSet

	data, err := json.Marshal(set)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if got, want := string(data), `[]`; got != want {
		t.Fatalf("MarshalJSON() = %s, want %s", got, want)
	}

	var roundTripped AliasSet
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if got := roundTripped.Len(); got != 0 {
		t.Fatalf("roundTripped.Len() = %d, want 0", got)
	}
}

func TestNewRelationshipType(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "valid", input: "runs-on"},
		{name: "empty rejected", input: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewRelationshipType(tt.input)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidArgument) {
					t.Errorf("got %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != RelationshipType(tt.input) {
				t.Errorf("got %q, want %q", got, tt.input)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CollectionName
// ---------------------------------------------------------------------------

func TestNewCollectionName(t *testing.T) {
	cn := NewCollectionName("clusters")
	if cn != "clusters" {
		t.Errorf("NewCollectionName = %q, want clusters", cn)
	}
	if cn.CollectionID() != "clusters" {
		t.Errorf("CollectionID() = %q, want clusters", cn.CollectionID())
	}
	if _, ok := cn.Parent(); ok {
		t.Error("flat CollectionName should have no parent")
	}
}

func TestParseCollectionName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "flat", input: "clusters"},
		{name: "flat camelCase", input: "userEvents"},
		{name: "nested", input: "publishers/123/books"},
		{name: "nested camelCase tail", input: "publishers/123/bookEditions"},
		{name: "empty rejected", input: "", wantErr: true},
		{name: "UpperCamelCase tail rejected", input: "publishers/123/BookEditions", wantErr: true},
		{name: "leading slash rejected", input: "/clusters", wantErr: true},
		{name: "trailing slash rejected", input: "clusters/", wantErr: true},
		{name: "double slash rejected", input: "publishers//books", wantErr: true},
		{name: "even segments rejected", input: "publishers/123", wantErr: true},
		{name: "four segments rejected", input: "publishers/123/books/les-mis", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseCollectionName(tt.input)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidArgument) {
					t.Errorf("got %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCollectionName_NestedParent(t *testing.T) {
	cn := CollectionName("publishers/123/books")
	if cn.CollectionID() != "books" {
		t.Errorf("CollectionID() = %q, want books", cn.CollectionID())
	}
	parent, ok := cn.Parent()
	if !ok {
		t.Fatal("nested CollectionName should have a parent")
	}
	if parent != "publishers/123" {
		t.Errorf("Parent() = %q, want publishers/123", parent)
	}
}

// ---------------------------------------------------------------------------
// ResourceName (renamed from RelativeResourceName)
// ---------------------------------------------------------------------------

func TestNewResourceName(t *testing.T) {
	tests := []struct {
		name       string
		collection CollectionName
		id         ResourceID
	}{
		{name: "flat", collection: "clusters", id: "prod"},
		{name: "nested", collection: "publishers/123/books", id: "les-mis"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewResourceName(tt.collection, tt.id)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.CollectionID() != tt.collection.CollectionID() {
				t.Errorf("CollectionID() = %q, want %q", got.CollectionID(), tt.collection.CollectionID())
			}
			if got.ID() != tt.id {
				t.Errorf("ID() = %q, want %q", got.ID(), tt.id)
			}
			if got.Collection() != tt.collection {
				t.Errorf("Collection() = %q, want %q", got.Collection(), tt.collection)
			}
		})
	}
}

func TestParseResourceName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "valid flat", input: "clusters/prod"},
		{name: "valid nested", input: "publishers/123/books/les-mis"},
		{name: "empty rejected", input: "", wantErr: true},
		{name: "no slash rejected", input: "prod", wantErr: true},
		{name: "trailing slash rejected", input: "clusters/", wantErr: true},
		{name: "leading slash rejected", input: "/clusters/prod", wantErr: true},
		{name: "double slash rejected", input: "clusters//prod", wantErr: true},
		{name: "odd segments rejected", input: "publishers/123/books", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseResourceName(tt.input)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidArgument) {
					t.Errorf("got %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(got) != tt.input {
				t.Errorf("got %q, want %q", got, tt.input)
			}
		})
	}
}

func TestFullResourceName_ConstructsAndParses(t *testing.T) {
	frn := NewFullResourceName("kind.fleetshift.io", "clusters/prod")

	if string(frn) != "//kind.fleetshift.io/clusters/prod" {
		t.Errorf("FullResourceName = %q, want //kind.fleetshift.io/clusters/prod", frn)
	}
	if frn.ServiceName() != "kind.fleetshift.io" {
		t.Errorf("ServiceName() = %q, want kind.fleetshift.io", frn.ServiceName())
	}
	if frn.ResourceName() != "clusters/prod" {
		t.Errorf("ResourceName() = %q, want clusters/prod", frn.ResourceName())
	}
}

func TestServiceName_FullName(t *testing.T) {
	sn := ServiceName("kind.fleetshift.io")
	got := sn.FullName("clusters/prod")
	want := FullResourceName("//kind.fleetshift.io/clusters/prod")

	if got != want {
		t.Errorf("ServiceName.FullName() = %q, want %q", got, want)
	}
}

func TestResourceName_FullName(t *testing.T) {
	rn := ResourceName("clusters/prod")
	got := rn.FullName("kind.fleetshift.io")
	want := FullResourceName("//kind.fleetshift.io/clusters/prod")

	if got != want {
		t.Errorf("ResourceName.FullName() = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// PlatformResource aggregate mutation methods
//
// PlatformResource has no surrogate UID -- ResourceName is its sole
// identifier (see resource_identity.go's package-level doc and
// docs/design/architecture/resource_identity_and_api.md). There is no
// PlatformResourceUID type to test here.
// ---------------------------------------------------------------------------

func TestPlatformResource_SetLabels(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("clusters/prod", map[string]string{"a": "1"}, now)

	later := now.Add(time.Hour)
	r.SetLabels(map[string]string{"b": "2"}, later)

	if r.Labels()["b"] != "2" {
		t.Errorf("Labels[b] = %q, want 2", r.Labels()["b"])
	}
	if _, ok := r.Labels()["a"]; ok {
		t.Error("Labels[a] should be gone after SetLabels")
	}
	if !r.UpdatedAt().Equal(later) {
		t.Errorf("UpdatedAt = %v, want %v", r.UpdatedAt(), later)
	}
	if !r.CreatedAt().Equal(now) {
		t.Errorf("CreatedAt changed: got %v, want %v", r.CreatedAt(), now)
	}
}

// Representation attach/update/delete behavior no longer lives on
// PlatformResource -- representations are derived on read by the
// repository (joining extension resources to platform resources by
// name); see resourceidentityrepotest.Run's representation-related
// subtests for the corresponding coverage at that layer.

func TestPlatformResource_AddAlias(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("clusters/prod", nil, now)

	alias, _ := NewAlias("gcp", "project_id", "my-proj")
	if err := r.AddAlias(alias); err != nil {
		t.Fatalf("AddAlias: %v", err)
	}

	aliases := collectAliasSet(r.Aliases())
	if len(aliases) != 1 {
		t.Fatalf("len(Aliases) = %d, want 1", len(aliases))
	}
	if aliases[0].Namespace() != "gcp" {
		t.Errorf("Namespace = %q, want gcp", aliases[0].Namespace())
	}
}

func TestPlatformResource_AddAlias_Idempotent(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("clusters/prod", nil, now)

	alias, _ := NewAlias("gcp", "project_id", "my-proj")
	if err := r.AddAlias(alias); err != nil {
		t.Fatalf("first AddAlias: %v", err)
	}
	if err := r.AddAlias(alias); err != nil {
		t.Fatalf("second AddAlias (idempotent): %v", err)
	}

	if r.Aliases().Len() != 1 {
		t.Errorf("len(Aliases) = %d, want 1 (idempotent)", r.Aliases().Len())
	}
}

func TestPlatformResource_AddAlias_RejectsConflictingValue(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("clusters/prod", nil, now)

	first, _ := NewAlias("gcp", "project_id", "proj-a")
	if err := r.AddAlias(first); err != nil {
		t.Fatalf("first AddAlias: %v", err)
	}

	conflicting, _ := NewAlias("gcp", "project_id", "proj-b")
	err := r.AddAlias(conflicting)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("conflicting alias: got %v, want ErrInvalidArgument", err)
	}

	aliases := collectAliasSet(r.Aliases())
	if len(aliases) != 1 {
		t.Fatalf("len(Aliases) = %d, want 1", len(aliases))
	}
	if aliases[0].Value() != "proj-a" {
		t.Errorf("Value = %q, want proj-a (unchanged)", aliases[0].Value())
	}
}

func TestPlatformResource_AddAlias_AllowsDifferentKeysInSameNamespace(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("clusters/prod", nil, now)

	a1, _ := NewAlias("gcp", "project_id", "proj-a")
	a2, _ := NewAlias("gcp", "zone", "us-central1-a")
	if err := r.AddAlias(a1); err != nil {
		t.Fatalf("first AddAlias: %v", err)
	}
	if err := r.AddAlias(a2); err != nil {
		t.Fatalf("second AddAlias: %v", err)
	}

	if r.Aliases().Len() != 2 {
		t.Errorf("len(Aliases) = %d, want 2", r.Aliases().Len())
	}
}

func TestPlatformResource_AddRelationship(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("clusters/prod", nil, now)

	err := r.AddRelationship(NewResourceRelationship("clusters/prod", "runs-on", "clusters/host", "kind.fleetshift.io", now))
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	rels := r.Relationships()
	if len(rels) != 1 {
		t.Fatalf("len(Relationships) = %d, want 1", len(rels))
	}
	if rels[0].Type() != "runs-on" {
		t.Errorf("Type = %q, want runs-on", rels[0].Type())
	}
}

func TestPlatformResource_AddRelationship_RejectsEmptyType(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("clusters/prod", nil, now)

	err := r.AddRelationship(NewResourceRelationship("clusters/prod", "", "clusters/host", "kind.fleetshift.io", now))
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("empty type: got %v, want ErrInvalidArgument", err)
	}
}

func TestPlatformResource_AddRelationship_RejectsForeignSourceName(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("clusters/prod", nil, now)

	err := r.AddRelationship(NewResourceRelationship("clusters/other", "runs-on", "clusters/host", "kind.fleetshift.io", now))
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("foreign source name: got %v, want ErrInvalidArgument", err)
	}
	if len(r.Relationships()) != 0 {
		t.Error("relationship should not have been added")
	}
}

func TestPlatformResource_EffectiveLabels(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("clusters/prod", map[string]string{"env": "prod", "team": "infra"}, now)

	got := r.EffectiveLabels()

	assertEq(t, "env", got["env"], "prod")
	assertEq(t, "team", got["team"], "infra")
	if len(got) != 2 {
		t.Errorf("len(EffectiveLabels) = %d, want 2", len(got))
	}
}

func TestPlatformResource_EffectiveLabels_ReturnsCopy(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("clusters/prod", map[string]string{"env": "prod"}, now)

	got := r.EffectiveLabels()
	got["env"] = "mutated"
	assertEq(t, "original unchanged", r.EffectiveLabels()["env"], "prod")
}
