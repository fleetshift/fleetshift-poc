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
