package phase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ocp-engine/internal/output"
)

func TestPhaseRequiresDestroy(t *testing.T) {
	phases := AllPhases()
	for _, p := range phases[:5] {
		if p.RequiresDestroyOnFailure {
			t.Errorf("phase %q should not require destroy on failure", p.Name)
		}
	}
	if !phases[5].RequiresDestroyOnFailure {
		t.Error("cluster phase should require destroy on failure")
	}
}

func TestRunPhase_Success(t *testing.T) {
	var buf bytes.Buffer
	p := Phase{
		Name:                     "test-phase",
		RequiresDestroyOnFailure: false,
	}
	err := RunPhase(p, func() error { return nil }, &buf, 1)
	if err != nil {
		t.Fatalf("RunPhase: %v", err)
	}
	var result output.PhaseResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if result.Phase != "test-phase" {
		t.Errorf("phase = %q, want test-phase", result.Phase)
	}
	if result.Status != "complete" {
		t.Errorf("status = %q, want complete", result.Status)
	}
}

func TestRunPhase_Failure(t *testing.T) {
	var buf bytes.Buffer
	p := Phase{
		Name:                     "cluster",
		RequiresDestroyOnFailure: true,
	}
	err := RunPhase(p, func() error {
		return fmt.Errorf("bootstrap timeout")
	}, &buf, 1)
	if err == nil {
		t.Fatal("expected error from RunPhase")
	}
	var result output.PhaseResult
	if jsonErr := json.Unmarshal(buf.Bytes(), &result); jsonErr != nil {
		t.Fatalf("invalid JSON: %v", jsonErr)
	}
	if result.Status != "failed" {
		t.Errorf("status = %q, want failed", result.Status)
	}
	if result.RequiresDestroy != true {
		t.Error("requires_destroy should be true for cluster phase")
	}
	if result.Error != "bootstrap timeout" {
		t.Errorf("error = %q, want bootstrap timeout", result.Error)
	}
}
