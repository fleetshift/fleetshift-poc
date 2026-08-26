package harness

import (
	"testing"
	"time"
)

func TestPollLog_ThrottlesIdenticalAndLogsChanges(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var logs []string
	p := &PollLog{
		now:      func() time.Time { return now },
		interval: pollLogInterval,
		print:    func(msg string) { logs = append(logs, msg) },
	}

	p.Logf("state=%s", "CREATING")
	p.Logf("state=%s", "CREATING")
	if len(logs) != 1 || logs[0] != "state=CREATING" {
		t.Fatalf("first distinct log: got %v", logs)
	}

	now = now.Add(pollLogInterval)
	p.Logf("state=%s", "CREATING")
	if len(logs) != 2 {
		t.Fatalf("after interval: got %v", logs)
	}

	p.Logf("state=%s", "ACTIVE")
	if len(logs) != 3 || logs[2] != "state=ACTIVE" {
		t.Fatalf("state change: got %v", logs)
	}
}

func TestStderrPollLog_Writes(t *testing.T) {
	t.Parallel()
	p := StderrPollLog()
	if p.print == nil || p.now == nil {
		t.Fatal("StderrPollLog missing print or now")
	}
	if p.interval != pollLogInterval {
		t.Fatalf("interval = %s, want %s", p.interval, pollLogInterval)
	}
}

func TestStartStderrHeartbeat_StopIsIdempotent(t *testing.T) {
	t.Parallel()
	stop := startStderrHeartbeat("still building AIO image")
	stop()
	stop()
}

func TestPollLog_Nil(t *testing.T) {
	t.Parallel()
	var p *PollLog
	p.Logf("ignored")
}
