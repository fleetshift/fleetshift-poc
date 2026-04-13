package output

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

func TestWriteError(t *testing.T) {
	var buf bytes.Buffer
	origErr := fmt.Errorf("something broke")
	returned := WriteError(&buf, "config_error", origErr, false)

	if returned != origErr {
		t.Error("WriteError should return the original error")
	}

	var got ErrorResult
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if got.Category != "config_error" {
		t.Errorf("category = %q, want config_error", got.Category)
	}
	if got.Message != "something broke" {
		t.Errorf("message = %q, want 'something broke'", got.Message)
	}
	if got.RequiresDestroy != false {
		t.Error("requires_destroy should be false")
	}
}

func TestWriteError_RequiresDestroy(t *testing.T) {
	var buf bytes.Buffer
	WriteError(&buf, "phase_error", fmt.Errorf("cluster failed"), true)

	var got ErrorResult
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if got.RequiresDestroy != true {
		t.Error("requires_destroy should be true")
	}
}

func TestPhaseResult_IncludesAttempt(t *testing.T) {
	var buf bytes.Buffer
	WritePhaseResult(&buf, PhaseResult{
		Phase:          "cluster",
		Status:         "complete",
		ElapsedSeconds: 100,
		Attempt:        2,
	})
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["attempt"] != float64(2) {
		t.Errorf("attempt = %v, want 2", got["attempt"])
	}
}

func TestProvisionResult_IncludesAttempt(t *testing.T) {
	var buf bytes.Buffer
	WriteProvisionResult(&buf, ProvisionResult{
		Status:  "succeeded",
		InfraID: "test-abc",
		Attempt: 3,
	})
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["attempt"] != float64(3) {
		t.Errorf("attempt = %v, want 3", got["attempt"])
	}
}
