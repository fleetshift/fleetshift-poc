package sqlite

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestConditionsToJSON_DualLastTransitionTimeRepresentations(t *testing.T) {
	tm := time.Date(2026, 6, 1, 12, 0, 0, 500_000_000, time.UTC)
	cond, err := domain.NewCondition("Ready", domain.ConditionTrue, "AllGood", "ok", tm)
	if err != nil {
		t.Fatal(err)
	}
	b, err := conditionsToJSON([]domain.Condition{cond})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"lastTransitionTime":"2026-06-01T12:00:00.500Z"`) {
		t.Fatalf("conditions JSON = %s, want ProtoJSON .500Z", b)
	}
	wantNorm := formatTimestampNorm(tm)
	if !strings.Contains(string(b), `"_lastTransitionTimeNorm":"`+wantNorm+`"`) {
		t.Fatalf("conditions JSON = %s, want fixed-width norm %q", b, wantNorm)
	}

	legacy := []byte(`{"Ready":{"status":"True","reason":"x","message":"y","lastTransitionTime":"2026-06-01T12:00:00.5Z"}}`)
	snaps, err := unmarshalConditionSnapshots(legacy)
	if err != nil {
		t.Fatal(err)
	}
	out, err := conditionSnapshotsToJSON(snaps)
	if err != nil {
		t.Fatal(err)
	}
	var byType map[string]ConditionJSON
	if err := json.Unmarshal(out, &byType); err != nil {
		t.Fatal(err)
	}
	c := byType["Ready"]
	raw, err := json.Marshal(c.LastTransitionTime)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `"2026-06-01T12:00:00.500Z"` {
		t.Fatalf("re-marshaled lastTransitionTime = %s, want .500Z", raw)
	}
	if c.LastTransitionTimeNorm != formatTimestampNorm(c.LastTransitionTime.Time()) {
		t.Fatalf("norm = %q, want derived from ProtoJSON instant", c.LastTransitionTimeNorm)
	}
}
