package domain

import (
	"errors"
	"testing"
	"time"
)

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

func TestPlatformResource_EffectiveLabels(t *testing.T) {
	platformLabels := map[string]string{"env": "prod", "team": "infra"}
	reps := []ResourceRepresentation{
		{
			ServiceName: "kind.fleetshift.io",
			Labels:      map[string]string{"version": "1.29", "runtime": "containerd"},
		},
		{
			ServiceName: "gcp.fleetshift.io",
			Labels:      map[string]string{"project": "my-proj"},
		},
	}

	got := EffectiveLabels(platformLabels, reps)

	// Platform labels are unprefixed.
	assertEq(t, "env", got["env"], "prod")
	assertEq(t, "team", got["team"], "infra")

	// Representation labels are service-prefixed.
	assertEq(t, "kind version", got["kind.fleetshift.io/version"], "1.29")
	assertEq(t, "kind runtime", got["kind.fleetshift.io/runtime"], "containerd")
	assertEq(t, "gcp project", got["gcp.fleetshift.io/project"], "my-proj")

	// Total count: 2 platform + 2 kind + 1 gcp = 5
	if len(got) != 5 {
		t.Errorf("len(EffectiveLabels) = %d, want 5", len(got))
	}
}

func TestPlatformResource_EffectiveLabels_PlatformOverrides(t *testing.T) {
	platformLabels := map[string]string{"kind.fleetshift.io/version": "override"}
	reps := []ResourceRepresentation{
		{
			ServiceName: "kind.fleetshift.io",
			Labels:      map[string]string{"version": "1.29"},
		},
	}

	got := EffectiveLabels(platformLabels, reps)

	// Platform label should take priority over the prefixed representation label.
	assertEq(t, "override", got["kind.fleetshift.io/version"], "override")
}

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
