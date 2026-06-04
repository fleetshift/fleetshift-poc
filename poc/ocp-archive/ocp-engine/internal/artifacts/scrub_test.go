package artifacts

import "testing"

func TestScrubJSON_RedactsPasswords(t *testing.T) {
	input := map[string]any{
		"infraID":  "test-abc",
		"password": "super-secret",
		"nested": map[string]any{
			"token":   "bearer-xyz",
			"cluster": "my-cluster",
		},
	}

	got := ScrubJSON(input)

	if got["password"] != "[REDACTED]" {
		t.Errorf("password = %v, want [REDACTED]", got["password"])
	}
	if got["infraID"] != "test-abc" {
		t.Errorf("infraID = %v, want test-abc", got["infraID"])
	}

	nested := got["nested"].(map[string]any)
	if nested["token"] != "[REDACTED]" {
		t.Errorf("nested.token = %v, want [REDACTED]", nested["token"])
	}
	if nested["cluster"] != "my-cluster" {
		t.Errorf("nested.cluster = %v, want my-cluster", nested["cluster"])
	}
}

func TestScrubJSON_HandlesNilValues(t *testing.T) {
	input := map[string]any{
		"key":      "value",
		"password": nil,
	}

	got := ScrubJSON(input)
	if got["password"] != "[REDACTED]" {
		t.Errorf("nil password should still be redacted, got %v", got["password"])
	}
}

func TestScrubJSON_EmptyMap(t *testing.T) {
	input := map[string]any{}
	got := ScrubJSON(input)
	if len(got) != 0 {
		t.Errorf("expected empty map, got %v", got)
	}
}
