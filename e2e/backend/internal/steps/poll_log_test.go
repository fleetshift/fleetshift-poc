package steps

import (
	"testing"
	"time"
)

func TestPollLog_ThrottlesIdenticalAndLogsChanges(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var logs []string
	p := &pollLog{
		now:      func() time.Time { return now },
		interval: pollLogInterval,
		print:    func(msg string) { logs = append(logs, msg) },
	}

	p.logf("state=%s", "CREATING")
	p.logf("state=%s", "CREATING")
	if len(logs) != 1 || logs[0] != "state=CREATING" {
		t.Fatalf("first distinct log: got %v", logs)
	}

	now = now.Add(pollLogInterval)
	p.logf("state=%s", "CREATING")
	if len(logs) != 2 {
		t.Fatalf("after interval: got %v", logs)
	}

	p.logf("state=%s", "ACTIVE")
	if len(logs) != 3 || logs[2] != "state=ACTIVE" {
		t.Fatalf("state change: got %v", logs)
	}
}
