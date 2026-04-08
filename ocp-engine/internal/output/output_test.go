package output

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestPhaseResult_Success(t *testing.T) {
	var buf bytes.Buffer
	WritePhaseResult(&buf, PhaseResult{
		Phase:          "manifests",
		Status:         "complete",
		ElapsedSeconds: 12,
	})

	var got PhaseResult
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if got.Phase != "manifests" {
		t.Errorf("phase = %q, want %q", got.Phase, "manifests")
	}
	if got.Status != "complete" {
		t.Errorf("status = %q, want %q", got.Status, "complete")
	}
	if got.ElapsedSeconds != 12 {
		t.Errorf("elapsed = %d, want %d", got.ElapsedSeconds, 12)
	}
}

func TestPhaseResult_Failure(t *testing.T) {
	var buf bytes.Buffer
	WritePhaseResult(&buf, PhaseResult{
		Phase:           "cluster",
		Status:          "failed",
		Error:           "bootstrap timeout",
		LogTail:         "some log lines...",
		ElapsedSeconds:  1802,
		RequiresDestroy: true,
	})

	var got PhaseResult
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if got.RequiresDestroy != true {
		t.Error("requires_destroy should be true")
	}
	if got.Error != "bootstrap timeout" {
		t.Errorf("error = %q, want %q", got.Error, "bootstrap timeout")
	}
}

func TestErrorResult(t *testing.T) {
	var buf bytes.Buffer
	WriteErrorResult(&buf, ErrorResult{
		Category:        "config_error",
		Message:         "missing pull secret",
		RequiresDestroy: false,
	})

	var got ErrorResult
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if got.Category != "config_error" {
		t.Errorf("category = %q, want %q", got.Category, "config_error")
	}
}

func TestStatusResult(t *testing.T) {
	var buf bytes.Buffer
	WriteStatusResult(&buf, StatusResult{
		State:           "running",
		CompletedPhases: []string{"extract", "install-config"},
		CurrentPhase:    "manifests",
		PID:             12345,
		PIDAlive:        true,
		HasKubeconfig:   false,
		HasMetadata:     false,
	})

	var got StatusResult
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if got.State != "running" {
		t.Errorf("state = %q, want %q", got.State, "running")
	}
	if len(got.CompletedPhases) != 2 {
		t.Errorf("completed_phases len = %d, want 2", len(got.CompletedPhases))
	}
	if got.PID != 12345 {
		t.Errorf("pid = %d, want 12345", got.PID)
	}
}

func TestDestroyResult(t *testing.T) {
	var buf bytes.Buffer
	WriteDestroyResult(&buf, DestroyResult{
		Action:         "destroy",
		Status:         "complete",
		InfraID:        "my-cluster-a1b2c",
		ElapsedSeconds: 300,
	})

	var got DestroyResult
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if got.InfraID != "my-cluster-a1b2c" {
		t.Errorf("infra_id = %q, want %q", got.InfraID, "my-cluster-a1b2c")
	}
}
