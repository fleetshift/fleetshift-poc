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

// ---------------------------------------------------------------------------
// PlatformResourceUID
// ---------------------------------------------------------------------------

func TestPlatformResourceUID_NewAndParse(t *testing.T) {
	uid := NewPlatformResourceUID()
	if uid.IsZero() {
		t.Fatal("NewPlatformResourceUID returned zero UUID")
	}

	s := uid.String()
	parsed, err := ParsePlatformResourceUID(s)
	if err != nil {
		t.Fatalf("ParsePlatformResourceUID(%q): %v", s, err)
	}
	if parsed != uid {
		t.Errorf("round-trip: got %s, want %s", parsed, uid)
	}
}

func TestPlatformResourceUID_TextRoundTrip(t *testing.T) {
	uid := NewPlatformResourceUID()
	text, err := uid.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText: %v", err)
	}
	var parsed PlatformResourceUID
	if err := parsed.UnmarshalText(text); err != nil {
		t.Fatalf("UnmarshalText: %v", err)
	}
	if parsed != uid {
		t.Errorf("round-trip: got %s, want %s", parsed, uid)
	}
}

func TestPlatformResourceUID_SQLRoundTrip(t *testing.T) {
	uid := NewPlatformResourceUID()
	val, err := uid.Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	var parsed PlatformResourceUID
	if err := parsed.Scan(val); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if parsed != uid {
		t.Errorf("round-trip: got %s, want %s", parsed, uid)
	}
}

// ---------------------------------------------------------------------------
// PlatformResource aggregate mutation methods
// ---------------------------------------------------------------------------

func TestPlatformResource_SetLabels(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewPlatformResourceUID()
	r := NewPlatformResource(uid, "clusters/prod", map[string]string{"a": "1"}, now)

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
	uid := NewPlatformResourceUID()
	r := NewPlatformResource(uid, "clusters/prod", nil, now)

	later := now.Add(time.Hour)
	err := r.AttachRepresentation(AttachRepresentationInput{
		ServiceName: "kind.fleetshift.io",
		Version:     "v1alpha1",
		Roles:       []RepresentationRole{RepresentationRoleManaged},
		Labels:      map[string]string{"runtime": "containerd"},
	}, later)
	if err != nil {
		t.Fatalf("AttachRepresentation: %v", err)
	}

	reps := r.Representations()
	if len(reps) != 1 {
		t.Fatalf("len(Representations) = %d, want 1", len(reps))
	}
	if reps[0].ServiceName() != "kind.fleetshift.io" {
		t.Errorf("ServiceName = %q, want kind.fleetshift.io", reps[0].ServiceName())
	}
	if reps[0].Version() != "v1alpha1" {
		t.Errorf("Version = %q, want v1alpha1", reps[0].Version())
	}
	if reps[0].Labels()["runtime"] != "containerd" {
		t.Errorf("Labels[runtime] = %q, want containerd", reps[0].Labels()["runtime"])
	}
	if reps[0].PlatformUID() != uid {
		t.Errorf("PlatformUID = %s, want %s", reps[0].PlatformUID(), uid)
	}
	if reps[0].Name() != "clusters/prod" {
		t.Errorf("Name = %q, want clusters/prod (inherited from aggregate)", reps[0].Name())
	}
}

func TestPlatformResource_AttachRepresentation_UpdatesExisting(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewPlatformResourceUID()
	r := NewPlatformResource(uid, "clusters/prod", nil, now)

	err := r.AttachRepresentation(AttachRepresentationInput{
		ServiceName: "kind.fleetshift.io",
		Version:     "v1alpha1",
		Roles:       []RepresentationRole{RepresentationRoleManaged},
		Labels:      map[string]string{"v": "1"},
	}, now)
	if err != nil {
		t.Fatalf("first attach: %v", err)
	}

	later := now.Add(time.Hour)
	err = r.AttachRepresentation(AttachRepresentationInput{
		ServiceName: "kind.fleetshift.io",
		Version:     "v1beta1",
		Roles:       []RepresentationRole{RepresentationRoleManaged, RepresentationRoleTarget},
		Labels:      map[string]string{"v": "2"},
	}, later)
	if err != nil {
		t.Fatalf("second attach: %v", err)
	}

	reps := r.Representations()
	if len(reps) != 1 {
		t.Fatalf("len(Representations) = %d, want 1 (upsert)", len(reps))
	}
	if reps[0].Version() != "v1beta1" {
		t.Errorf("Version = %q, want v1beta1", reps[0].Version())
	}
	if reps[0].Labels()["v"] != "2" {
		t.Errorf("Labels[v] = %q, want 2", reps[0].Labels()["v"])
	}
}

func TestPlatformResource_AttachRepresentation_RejectsInvalidRoles(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewPlatformResourceUID()
	r := NewPlatformResource(uid, "clusters/prod", nil, now)

	err := r.AttachRepresentation(AttachRepresentationInput{
		ServiceName: "kind.fleetshift.io",
		Version:     "v1",
		Roles:       []RepresentationRole{RepresentationRoleManaged, RepresentationRoleInventory},
	}, now)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("managed+inventory: got %v, want ErrInvalidArgument", err)
	}
}

func TestPlatformResource_DeleteRepresentation(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewPlatformResourceUID()
	r := NewPlatformResource(uid, "clusters/prod", nil, now)

	err := r.AttachRepresentation(AttachRepresentationInput{
		ServiceName: "kind.fleetshift.io",
		Version:     "v1",
		Roles:       []RepresentationRole{RepresentationRoleManaged},
	}, now)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}

	later := now.Add(time.Hour)
	err = r.DeleteRepresentation("kind.fleetshift.io", later)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	if len(r.Representations()) != 0 {
		t.Errorf("representations len = %d, want 0", len(r.Representations()))
	}

	all := r.AllRepresentations()
	if len(all) != 1 {
		t.Fatalf("all representations len = %d, want 1", len(all))
	}
	if !all[0].Deleted() {
		t.Fatal("Deleted is false, want true")
	}
}

func TestPlatformResource_DeleteRepresentation_NotFound(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewPlatformResourceUID()
	r := NewPlatformResource(uid, "clusters/prod", nil, now)

	err := r.DeleteRepresentation("missing.io", now)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("delete missing: got %v, want ErrNotFound", err)
	}
}

func TestPlatformResource_AddAlias(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewPlatformResourceUID()
	r := NewPlatformResource(uid, "clusters/prod", nil, now)

	alias, _ := NewAlias("gcp", "project_id", "my-proj")
	if err := r.AddAlias(alias); err != nil {
		t.Fatalf("AddAlias: %v", err)
	}

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
	uid := NewPlatformResourceUID()
	r := NewPlatformResource(uid, "clusters/prod", nil, now)

	alias, _ := NewAlias("gcp", "project_id", "my-proj")
	if err := r.AddAlias(alias); err != nil {
		t.Fatalf("first AddAlias: %v", err)
	}
	if err := r.AddAlias(alias); err != nil {
		t.Fatalf("second AddAlias (idempotent): %v", err)
	}

	if len(r.Aliases()) != 1 {
		t.Errorf("len(Aliases) = %d, want 1 (idempotent)", len(r.Aliases()))
	}
}

func TestPlatformResource_AddAlias_RejectsConflictingValue(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewPlatformResourceUID()
	r := NewPlatformResource(uid, "clusters/prod", nil, now)

	first, _ := NewAlias("gcp", "project_id", "proj-a")
	if err := r.AddAlias(first); err != nil {
		t.Fatalf("first AddAlias: %v", err)
	}

	conflicting, _ := NewAlias("gcp", "project_id", "proj-b")
	err := r.AddAlias(conflicting)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("conflicting alias: got %v, want ErrInvalidArgument", err)
	}

	aliases := r.Aliases()
	if len(aliases) != 1 {
		t.Fatalf("len(Aliases) = %d, want 1", len(aliases))
	}
	if aliases[0].Value != "proj-a" {
		t.Errorf("Value = %q, want proj-a (unchanged)", aliases[0].Value)
	}
}

func TestPlatformResource_AddAlias_AllowsDifferentKeysInSameNamespace(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewPlatformResourceUID()
	r := NewPlatformResource(uid, "clusters/prod", nil, now)

	a1, _ := NewAlias("gcp", "project_id", "proj-a")
	a2, _ := NewAlias("gcp", "zone", "us-central1-a")
	if err := r.AddAlias(a1); err != nil {
		t.Fatalf("first AddAlias: %v", err)
	}
	if err := r.AddAlias(a2); err != nil {
		t.Fatalf("second AddAlias: %v", err)
	}

	if len(r.Aliases()) != 2 {
		t.Errorf("len(Aliases) = %d, want 2", len(r.Aliases()))
	}
}

func TestPlatformResource_AddRelationship(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid1 := NewPlatformResourceUID()
	uid2 := NewPlatformResourceUID()
	r := NewPlatformResource(uid1, "clusters/prod", nil, now)

	err := r.AddRelationship(NewResourceRelationship(uid1, "runs-on", uid2, "kind.fleetshift.io", now))
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
	uid1 := NewPlatformResourceUID()
	uid2 := NewPlatformResourceUID()
	r := NewPlatformResource(uid1, "clusters/prod", nil, now)

	err := r.AddRelationship(NewResourceRelationship(uid1, "", uid2, "kind.fleetshift.io", now))
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("empty type: got %v, want ErrInvalidArgument", err)
	}
}

func TestPlatformResource_AddRelationship_RejectsForeignSourceUID(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid1 := NewPlatformResourceUID()
	uid2 := NewPlatformResourceUID()
	foreignUID := NewPlatformResourceUID()
	r := NewPlatformResource(uid1, "clusters/prod", nil, now)

	err := r.AddRelationship(NewResourceRelationship(foreignUID, "runs-on", uid2, "kind.fleetshift.io", now))
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("foreign source UID: got %v, want ErrInvalidArgument", err)
	}
	if len(r.Relationships()) != 0 {
		t.Error("relationship should not have been added")
	}
}

func TestPlatformResource_EffectiveLabels(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewPlatformResourceUID()
	r := NewPlatformResource(uid, "clusters/prod", map[string]string{"env": "prod", "team": "infra"}, now)

	if err := r.AttachRepresentation(AttachRepresentationInput{
		ServiceName: "kind.fleetshift.io",
		Version:     "v1",
		Roles:       []RepresentationRole{RepresentationRoleManaged},
		Labels:      map[string]string{"version": "1.29", "runtime": "containerd"},
	}, now); err != nil {
		t.Fatalf("attach kind: %v", err)
	}
	if err := r.AttachRepresentation(AttachRepresentationInput{
		ServiceName: "gcp.fleetshift.io",
		Version:     "v1",
		Roles:       []RepresentationRole{RepresentationRoleInventory},
		Labels:      map[string]string{"project": "my-proj"},
	}, now); err != nil {
		t.Fatalf("attach gcp: %v", err)
	}

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
	uid := NewPlatformResourceUID()
	r := NewPlatformResource(uid, "clusters/prod", map[string]string{"kind.fleetshift.io/version": "override"}, now)

	if err := r.AttachRepresentation(AttachRepresentationInput{
		ServiceName: "kind.fleetshift.io",
		Version:     "v1",
		Roles:       []RepresentationRole{RepresentationRoleManaged},
		Labels:      map[string]string{"version": "1.29"},
	}, now); err != nil {
		t.Fatalf("attach: %v", err)
	}

	got := r.EffectiveLabels()
	assertEq(t, "override", got["kind.fleetshift.io/version"], "override")
}

func TestPlatformResource_EffectiveLabels_ExcludesDeletedRepresentations(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	uid := NewPlatformResourceUID()
	r := NewPlatformResource(uid, "clusters/prod", map[string]string{"env": "prod"}, now)

	if err := r.AttachRepresentation(AttachRepresentationInput{
		ServiceName: "kind.fleetshift.io",
		Version:     "v1",
		Roles:       []RepresentationRole{RepresentationRoleManaged},
		Labels:      map[string]string{"version": "1.29"},
	}, now); err != nil {
		t.Fatalf("attach: %v", err)
	}

	if err := r.DeleteRepresentation("kind.fleetshift.io", now.Add(time.Hour)); err != nil {
		t.Fatalf("delete: %v", err)
	}

	got := r.EffectiveLabels()
	if _, ok := got["kind.fleetshift.io/version"]; ok {
		t.Error("deleted representation labels should not appear in effective labels")
	}
	assertEq(t, "env", got["env"], "prod")
}
