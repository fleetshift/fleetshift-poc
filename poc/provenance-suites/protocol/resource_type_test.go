package protocol

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestParseResourceType(t *testing.T) {
	for _, value := range []string{"kind.fleetshift.io/Cluster", "OtherService/Cluster", "addon/OwnType", "extension.example/Type-v2"} {
		got, err := ParseResourceType(value)
		if err != nil || got.String() != value {
			t.Errorf("parse %q: %v, %v", value, got, err)
		}
		encoded, err := json.Marshal(got)
		if err != nil || string(encoded) != `"`+value+`"` {
			t.Fatalf("encode %q: %s, %v", value, encoded, err)
		}
		var decoded ResourceType
		if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != got {
			t.Fatalf("round trip %q: %v", value, err)
		}
		if map[ResourceType]bool{got: true}[decoded] != true {
			t.Fatal("resource type not a stable comparable key")
		}
	}
	for _, value := range []string{"", "clusters", "/Cluster", "kind.example/", "//kind.example/Cluster", "kind.example/v1/Cluster", "kind.example/Cluster/", "kind.example/Clus\nter", "kind.example/\u00a0Cluster"} {
		if _, err := ParseResourceType(value); !errors.Is(err, ErrMalformedEvidence) {
			t.Errorf("invalid type %q: %v", value, err)
		}
	}
	upper, _ := ParseResourceType("OtherService/Cluster")
	lower, _ := ParseResourceType("otherservice/Cluster")
	if upper == lower {
		t.Fatal("parser normalized a signed identity")
	}
}

func TestResourceTypeRejectsUnconstructedAndMalformedJSON(t *testing.T) {
	var value ResourceType
	if _, err := json.Marshal(value); !errors.Is(err, ErrMalformedEvidence) {
		t.Fatalf("zero encoding: %v", err)
	}
	for _, input := range []string{`null`, `""`, `"clusters"`, `42`, `{}`, `[]`} {
		if err := json.Unmarshal([]byte(input), &value); !errors.Is(err, ErrMalformedEvidence) {
			t.Errorf("decode %s: %v", input, err)
		}
	}
	for _, assertion := range []interface {
		Assertion() (TypedAssertion, error)
	}{ManagedResourceAuthorization{}, FulfillmentRelation{}} {
		if _, err := assertion.Assertion(); !errors.Is(err, ErrMalformedEvidence) {
			t.Errorf("zero predicate encoding: %v", err)
		}
	}
}

func resourceTypeForTest(t *testing.T, value string) ResourceType {
	t.Helper()
	parsed, err := ParseResourceType(value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestFailedResourceTypeDecodingPreservesConstructedValue(t *testing.T) {
	want := resourceTypeForTest(t, "kind.example/Cluster")
	value := want
	for _, input := range []string{`null`, `""`, `"clusters"`, `42`} {
		if err := json.Unmarshal([]byte(input), &value); !errors.Is(err, ErrMalformedEvidence) {
			t.Fatalf("decode %s: %v", input, err)
		}
		if value != want {
			t.Fatalf("failed parse %s changed resource type", input)
		}
	}
}
