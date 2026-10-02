package protocol

import (
	"context"
	"errors"
	"testing"
)

func TestSelectionRejectsMissingTrustedLookupMachinery(t *testing.T) {
	trust, evidence := selectionFixture(t)
	for _, tc := range []struct {
		name   string
		lookup TargetLookup
	}{
		{"missing lookup function", nil},
		{"successful nil implementation", func(ProvenanceType) (TargetAPI, bool) { return nil, true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, tc.lookup, defaultServices()); !errors.Is(err, ErrUnknownProvenanceType) {
				t.Fatalf("invalid trusted lookup=%v", err)
			}
		})
	}
}
