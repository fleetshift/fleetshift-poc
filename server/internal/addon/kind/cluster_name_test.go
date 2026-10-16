package kind

import (
	"strings"
	"testing"

	"github.com/stolostron/ocm-resource-manager/server/internal/domain"
)

func TestEncodeDecodeKindClusterName(t *testing.T) {
	name, err := encodeKindClusterName("demo")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if name != "rm--demo" {
		t.Fatalf("encode = %q, want rm--demo", name)
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
	listed := []string{"demo", "rm--other", "rm--demo"}
	got, found := findOwnedCluster(listed, "demo")
	if !found || got != "rm--demo" {
		t.Fatalf("findOwnedCluster = %q %v, want rm--demo true", got, found)
	}
	if _, found := findOwnedCluster(listed, "missing"); found {
		t.Fatal("expected missing id not found")
	}
}

func TestForeignClusterConflict(t *testing.T) {
	if !foreignClusterConflict([]string{"demo"}, "demo") {
		t.Fatal("bare demo should conflict")
	}
	if foreignClusterConflict([]string{"rm--demo"}, "demo") {
		t.Fatal("owned encoding should not conflict")
	}
}
