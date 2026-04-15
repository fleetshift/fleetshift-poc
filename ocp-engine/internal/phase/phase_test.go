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
	for _, p := range phases[:6] {
		if p.RequiresDestroyOnFailure {
			t.Errorf("phase %q should not require destroy on failure", p.Name)
		}
	}
	if !phases[6].RequiresDestroyOnFailure {
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

func TestAllPhases_CCOctlBetweenExtractAndInstallConfig(t *testing.T) {
	phases := AllPhases()

	// Find positions of extract, ccoctl, and install-config
	extractIdx := -1
	ccoctlIdx := -1
	installConfigIdx := -1

	for i, p := range phases {
		switch p.Name {
		case "extract":
			extractIdx = i
		case "ccoctl":
			ccoctlIdx = i
		case "install-config":
			installConfigIdx = i
		}
	}

	if ccoctlIdx == -1 {
		t.Fatal("ccoctl phase not found in AllPhases()")
	}

	if extractIdx == -1 {
		t.Fatal("extract phase not found in AllPhases()")
	}

	if installConfigIdx == -1 {
		t.Fatal("install-config phase not found in AllPhases()")
	}

	if ccoctlIdx <= extractIdx {
		t.Errorf("ccoctl phase at index %d should come after extract phase at index %d", ccoctlIdx, extractIdx)
	}

	if ccoctlIdx >= installConfigIdx {
		t.Errorf("ccoctl phase at index %d should come before install-config phase at index %d", ccoctlIdx, installConfigIdx)
	}
}

func TestAllPhases_CCOctlDoesNotRequireDestroy(t *testing.T) {
	phases := AllPhases()

	for _, p := range phases {
		if p.Name == "ccoctl" {
			if p.RequiresDestroyOnFailure {
				t.Error("ccoctl phase should not require destroy on failure")
			}
			return
		}
	}

	t.Fatal("ccoctl phase not found in AllPhases()")
}
