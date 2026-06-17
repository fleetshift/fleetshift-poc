package domain

import (
	"errors"
	"testing"
	"time"
)

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
		{name: "valid", input: "clusters"},
		{name: "empty rejected", input: "", wantErr: true},
		{name: "uppercase rejected", input: "Clusters", wantErr: true},
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
			if got.Namespace != tt.ns || got.Key != tt.key || got.Value != tt.value {
				t.Errorf("got %+v, want ns=%q key=%q value=%q", got, tt.ns, tt.key, tt.value)
			}
		})
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
// RelativeResourceName (existing tests, preserved)
// ---------------------------------------------------------------------------

func TestRelativeResourceName_ValidatesCollectionQualifiedName(t *testing.T) {
	tests := []struct {
		name       string
		collection CollectionID
		id         string
		wantErr    bool
	}{
		{name: "valid", collection: "clusters", id: "prod", wantErr: false},
		{name: "empty collection", collection: "", id: "prod", wantErr: true},
		{name: "empty id", collection: "clusters", id: "", wantErr: true},
		{name: "uppercase collection", collection: "Clusters", id: "prod", wantErr: true},
		{name: "collection with slash", collection: "a/b", id: "prod", wantErr: true},
		{name: "id with slash", collection: "clusters", id: "a/b", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewRelativeResourceName(tt.collection, tt.id)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidArgument) {
					t.Errorf("got err %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.CollectionID() != tt.collection {
				t.Errorf("CollectionID() = %q, want %q", got.CollectionID(), tt.collection)
			}
			if got.ID() != tt.id {
				t.Errorf("ID() = %q, want %q", got.ID(), tt.id)
			}
		})
	}
}

func TestRelativeResourceName_Validate(t *testing.T) {
	tests := []struct {
		name    string
		input   RelativeResourceName
		wantErr bool
	}{
		{name: "valid", input: "clusters/prod", wantErr: false},
		{name: "empty", input: "", wantErr: true},
		{name: "no slash", input: "clusters", wantErr: true},
		{name: "empty collection", input: "/prod", wantErr: true},
		{name: "empty id", input: "clusters/", wantErr: true},
		{name: "uppercase collection", input: "Clusters/prod", wantErr: true},
		{name: "multi-segment id", input: "clusters/a/b", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRelativeResourceName(tt.input)
			if tt.wantErr && !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("got err %v, want ErrInvalidArgument", err)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
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
	if frn.RelativeName() != "clusters/prod" {
		t.Errorf("RelativeName() = %q, want clusters/prod", frn.RelativeName())
	}
}

func TestRepresentationRoles_Validate(t *testing.T) {
	tests := []struct {
		name    string
		roles   []RepresentationRole
		wantErr bool
	}{
		{name: "managed only", roles: []RepresentationRole{RepresentationRoleManaged}, wantErr: false},
		{name: "inventory only", roles: []RepresentationRole{RepresentationRoleInventory}, wantErr: false},
		{name: "target only", roles: []RepresentationRole{RepresentationRoleTarget}, wantErr: false},
		{name: "target + inventory", roles: []RepresentationRole{RepresentationRoleTarget, RepresentationRoleInventory}, wantErr: false},
		{name: "target + managed", roles: []RepresentationRole{RepresentationRoleTarget, RepresentationRoleManaged}, wantErr: false},
		{name: "managed + inventory rejected", roles: []RepresentationRole{RepresentationRoleManaged, RepresentationRoleInventory}, wantErr: true},
		{name: "empty rejected", roles: []RepresentationRole{}, wantErr: true},
		{name: "unknown role rejected", roles: []RepresentationRole{"unknown"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRepresentationRoles(tt.roles)
			if tt.wantErr && !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("got err %v, want ErrInvalidArgument", err)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// PlatformResource aggregate mutation methods
// ---------------------------------------------------------------------------

func TestPlatformResource_SetLabels(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", map[string]string{"a": "1"}, now)

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

func TestPlatformResource_AttachRepresentation(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", nil, now)

	later := now.Add(time.Hour)
	err := r.AttachRepresentation(AttachRepresentationInput{
		ServiceName:  "kind.fleetshift.io",
		Version:      "v1alpha1",
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
		Roles:        []RepresentationRole{RepresentationRoleManaged},
		Labels:       map[string]string{"runtime": "containerd"},
	}, later)
	if err != nil {
		t.Fatalf("AttachRepresentation: %v", err)
	}

	reps := r.Representations()
	if len(reps) != 1 {
		t.Fatalf("len(Representations) = %d, want 1", len(reps))
	}
	if reps[0].ServiceName != "kind.fleetshift.io" {
		t.Errorf("ServiceName = %q, want kind.fleetshift.io", reps[0].ServiceName)
	}
	if reps[0].Version != "v1alpha1" {
		t.Errorf("Version = %q, want v1alpha1", reps[0].Version)
	}
	if reps[0].Labels["runtime"] != "containerd" {
		t.Errorf("Labels[runtime] = %q, want containerd", reps[0].Labels["runtime"])
	}
	if reps[0].PlatformUID != "uid-1" {
		t.Errorf("PlatformUID = %q, want uid-1", reps[0].PlatformUID)
	}
}

func TestPlatformResource_AttachRepresentation_UpdatesExisting(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", nil, now)

	err := r.AttachRepresentation(AttachRepresentationInput{
		ServiceName:  "kind.fleetshift.io",
		Version:      "v1alpha1",
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
		Roles:        []RepresentationRole{RepresentationRoleManaged},
		Labels:       map[string]string{"v": "1"},
	}, now)
	if err != nil {
		t.Fatalf("first attach: %v", err)
	}

	later := now.Add(time.Hour)
	err = r.AttachRepresentation(AttachRepresentationInput{
		ServiceName:  "kind.fleetshift.io",
		Version:      "v1beta1",
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
		Roles:        []RepresentationRole{RepresentationRoleManaged, RepresentationRoleTarget},
		Labels:       map[string]string{"v": "2"},
	}, later)
	if err != nil {
		t.Fatalf("second attach: %v", err)
	}

	reps := r.Representations()
	if len(reps) != 1 {
		t.Fatalf("len(Representations) = %d, want 1 (upsert)", len(reps))
	}
	if reps[0].Version != "v1beta1" {
		t.Errorf("Version = %q, want v1beta1", reps[0].Version)
	}
	if reps[0].Labels["v"] != "2" {
		t.Errorf("Labels[v] = %q, want 2", reps[0].Labels["v"])
	}
}

func TestPlatformResource_AttachRepresentation_RejectsCollectionMismatch(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", nil, now)

	err := r.AttachRepresentation(AttachRepresentationInput{
		ServiceName:  "kind.fleetshift.io",
		Version:      "v1",
		CollectionID: "nodes",
		RelativeName: "clusters/prod",
		Roles:        []RepresentationRole{RepresentationRoleManaged},
	}, now)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("mismatched collection: got %v, want ErrInvalidArgument", err)
	}
}

func TestPlatformResource_AttachRepresentation_RejectsInvalidRoles(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", nil, now)

	err := r.AttachRepresentation(AttachRepresentationInput{
		ServiceName:  "kind.fleetshift.io",
		Version:      "v1",
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
		Roles:        []RepresentationRole{RepresentationRoleManaged, RepresentationRoleInventory},
	}, now)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("managed+inventory: got %v, want ErrInvalidArgument", err)
	}
}

func TestPlatformResource_TombstoneRepresentation(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", nil, now)

	err := r.AttachRepresentation(AttachRepresentationInput{
		ServiceName:  "kind.fleetshift.io",
		Version:      "v1",
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
		Roles:        []RepresentationRole{RepresentationRoleManaged},
	}, now)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}

	later := now.Add(time.Hour)
	err = r.TombstoneRepresentation("kind.fleetshift.io", "clusters", "clusters/prod", later)
	if err != nil {
		t.Fatalf("tombstone: %v", err)
	}

	// Active representations should be empty.
	if len(r.Representations()) != 0 {
		t.Errorf("active representations len = %d, want 0", len(r.Representations()))
	}

	// AllRepresentations (including tombstoned) should still have it.
	all := r.AllRepresentations()
	if len(all) != 1 {
		t.Fatalf("all representations len = %d, want 1", len(all))
	}
	if all[0].DeletedAt == nil {
		t.Fatal("DeletedAt is nil, want non-nil")
	}
}

func TestPlatformResource_TombstoneRepresentation_NotFound(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", nil, now)

	err := r.TombstoneRepresentation("missing.io", "clusters", "clusters/prod", now)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("tombstone missing: got %v, want ErrNotFound", err)
	}
}

func TestPlatformResource_AddAlias(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", nil, now)

	alias, _ := NewAlias("gcp", "project_id", "my-proj")
	r.AddAlias(alias)

	aliases := r.Aliases()
	if len(aliases) != 1 {
		t.Fatalf("len(Aliases) = %d, want 1", len(aliases))
	}
	if aliases[0].Namespace != "gcp" {
		t.Errorf("Namespace = %q, want gcp", aliases[0].Namespace)
	}
}

func TestPlatformResource_AddAlias_Idempotent(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", nil, now)

	alias, _ := NewAlias("gcp", "project_id", "my-proj")
	r.AddAlias(alias)
	r.AddAlias(alias)

	if len(r.Aliases()) != 1 {
		t.Errorf("len(Aliases) = %d, want 1 (idempotent)", len(r.Aliases()))
	}
}

func TestPlatformResource_AddRelationship(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", nil, now)

	err := r.AddRelationship(ResourceRelationship{
		SourceUID:     "uid-1",
		Type:          "runs-on",
		TargetUID:     "uid-2",
		SourceService: "kind.fleetshift.io",
		CreatedAt:     now,
	})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	rels := r.Relationships()
	if len(rels) != 1 {
		t.Fatalf("len(Relationships) = %d, want 1", len(rels))
	}
	if rels[0].Type != "runs-on" {
		t.Errorf("Type = %q, want runs-on", rels[0].Type)
	}
}

func TestPlatformResource_AddRelationship_RejectsEmptyType(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", nil, now)

	err := r.AddRelationship(ResourceRelationship{
		SourceUID:     "uid-1",
		Type:          "",
		TargetUID:     "uid-2",
		SourceService: "kind.fleetshift.io",
		CreatedAt:     now,
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("empty type: got %v, want ErrInvalidArgument", err)
	}
}

func TestPlatformResource_EffectiveLabels(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", map[string]string{"env": "prod", "team": "infra"}, now)

	_ = r.AttachRepresentation(AttachRepresentationInput{
		ServiceName:  "kind.fleetshift.io",
		Version:      "v1",
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
		Roles:        []RepresentationRole{RepresentationRoleManaged},
		Labels:       map[string]string{"version": "1.29", "runtime": "containerd"},
	}, now)
	_ = r.AttachRepresentation(AttachRepresentationInput{
		ServiceName:  "gcp.fleetshift.io",
		Version:      "v1",
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
		Roles:        []RepresentationRole{RepresentationRoleInventory},
		Labels:       map[string]string{"project": "my-proj"},
	}, now)

	got := r.EffectiveLabels()

	assertEq(t, "env", got["env"], "prod")
	assertEq(t, "team", got["team"], "infra")
	assertEq(t, "kind version", got["kind.fleetshift.io/version"], "1.29")
	assertEq(t, "kind runtime", got["kind.fleetshift.io/runtime"], "containerd")
	assertEq(t, "gcp project", got["gcp.fleetshift.io/project"], "my-proj")
	if len(got) != 5 {
		t.Errorf("len(EffectiveLabels) = %d, want 5", len(got))
	}
}

func TestPlatformResource_EffectiveLabels_PlatformOverrides(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", map[string]string{"kind.fleetshift.io/version": "override"}, now)

	_ = r.AttachRepresentation(AttachRepresentationInput{
		ServiceName:  "kind.fleetshift.io",
		Version:      "v1",
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
		Roles:        []RepresentationRole{RepresentationRoleManaged},
		Labels:       map[string]string{"version": "1.29"},
	}, now)

	got := r.EffectiveLabels()
	assertEq(t, "override", got["kind.fleetshift.io/version"], "override")
}

func TestPlatformResource_EffectiveLabels_ExcludesTombstoned(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	r := NewPlatformResource("uid-1", "clusters", "clusters/prod", map[string]string{"env": "prod"}, now)

	_ = r.AttachRepresentation(AttachRepresentationInput{
		ServiceName:  "kind.fleetshift.io",
		Version:      "v1",
		CollectionID: "clusters",
		RelativeName: "clusters/prod",
		Roles:        []RepresentationRole{RepresentationRoleManaged},
		Labels:       map[string]string{"version": "1.29"},
	}, now)

	_ = r.TombstoneRepresentation("kind.fleetshift.io", "clusters", "clusters/prod", now.Add(time.Hour))

	got := r.EffectiveLabels()
	if _, ok := got["kind.fleetshift.io/version"]; ok {
		t.Error("tombstoned representation labels should not appear in effective labels")
	}
	assertEq(t, "env", got["env"], "prod")
}

// ---------------------------------------------------------------------------
// Backward-compatibility: old Validate* functions still exist as aliases
// ---------------------------------------------------------------------------

func TestServiceName_Validate(t *testing.T) {
	if err := ValidateServiceName(""); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("empty: got %v, want ErrInvalidArgument", err)
	}
	if err := ValidateServiceName("a/b"); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("slash: got %v, want ErrInvalidArgument", err)
	}
	if err := ValidateServiceName("kind.fleetshift.io"); err != nil {
		t.Errorf("valid: unexpected error: %v", err)
	}
}

func TestAPIVersion_Validate(t *testing.T) {
	if err := ValidateAPIVersion(""); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("empty: got %v, want ErrInvalidArgument", err)
	}
	if err := ValidateAPIVersion("1alpha1"); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("no v prefix: got %v, want ErrInvalidArgument", err)
	}
	if err := ValidateAPIVersion("v1alpha1"); err != nil {
		t.Errorf("valid: unexpected error: %v", err)
	}
}
