package kind

import (
	"strings"
	"testing"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

func TestEncodeDecodeKindClusterName(t *testing.T) {
	name, err := encodeKindClusterName("demo")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if name != "ocm-fleet--demo" {
		t.Fatalf("encode = %q, want ocm-fleet--demo", name)
	}
	id, ok := decodeKindClusterName(name)
	if !ok || id != "demo" {
		t.Fatalf("decode = %q ok=%v, want demo true", id, ok)
	}
}

func TestEncodeKindClusterName_RejectsInvalid(t *testing.T) {
	cases := []string{
		"",
		"Demo",
		"has_underscore",
		"has/slash",
		strings.Repeat("a", maxKindClusterNameLen), // prefix pushes over limit
	}
	for _, id := range cases {
		if _, err := encodeKindClusterName(domain.ResourceID(id)); err == nil {
			t.Errorf("encode(%q) succeeded, want error", id)
		}
	}
}

func TestEncodeKindClusterName_LengthLimit(t *testing.T) {
	id := domain.ResourceID(strings.Repeat("a", 39))
	name, err := encodeKindClusterName(id)
	if err != nil {
		t.Fatalf("encode maximum-length id: %v", err)
	}
	if len(name) != maxKindClusterNameLen {
		t.Fatalf("encoded name length = %d, want %d", len(name), maxKindClusterNameLen)
	}
	if _, err := encodeKindClusterName(id + "a"); err == nil {
		t.Fatal("encode 40-character id succeeded, want error")
	}
}

func TestEncodeKindClusterName_AcceptsKindCharset(t *testing.T) {
	cases := []string{"demo", "ends-", "-starts", ".dotted.", "a.b-c"}
	for _, id := range cases {
		name, err := encodeKindClusterName(domain.ResourceID(id))
		if err != nil {
			t.Errorf("encode(%q): %v", id, err)
			continue
		}
		want := kindClusterNamePrefix + id
		if name != want {
			t.Errorf("encode(%q) = %q, want %q", id, name, want)
		}
	}
}

func TestFindOwnedCluster(t *testing.T) {
	listed := []string{"demo", "ocm-fleet--other", "ocm-fleet--demo"}
	got, found := findOwnedCluster(listed, "demo")
	if !found || got != "ocm-fleet--demo" {
		t.Fatalf("findOwnedCluster = %q %v, want ocm-fleet--demo true", got, found)
	}
	if _, found := findOwnedCluster(listed, "missing"); found {
		t.Fatal("expected missing id not found")
	}
}

func TestForeignClusterConflict(t *testing.T) {
	if !foreignClusterConflict([]string{"demo"}, "demo") {
		t.Fatal("bare demo should conflict")
	}
	if foreignClusterConflict([]string{"ocm-fleet--demo"}, "demo") {
		t.Fatal("owned encoding should not conflict")
	}
}
