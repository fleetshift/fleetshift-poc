package protocol

import (
	"errors"
	"testing"
)

func TestValidateFullResourceType(t *testing.T) {
	for _, value := range []string{"kind.fleetshift.io/Cluster", "OtherService/Cluster", "addon/OwnType", "extension.example/Type-v2"} {
		if err := ValidateResourceType(value); err != nil {
			t.Errorf("valid type %q: %v", value, err)
		}
	}
	for _, value := range []string{"", "clusters", "/Cluster", "kind.example/", "//kind.example/Cluster", "kind.example/v1/Cluster", "kind.example/Cluster/", "kind.example/Clus\nter", "kind.example/\u00a0Cluster"} {
		if err := ValidateResourceType(value); !errors.Is(err, ErrMalformedEvidence) {
			t.Errorf("invalid type %q: %v", value, err)
		}
	}
}
