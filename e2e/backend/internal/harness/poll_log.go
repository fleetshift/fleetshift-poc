package harness

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// PollLog writes throttled progress lines. Identical messages are withheld
// until interval elapses; a different message is always logged.
type PollLog struct {
	now      func() time.Time
	interval time.Duration
	lastAt   time.Time
	lastMsg  string
	print    func(string)
}

// NewPollLog returns a PollLog that writes to t.Log.
func NewPollLog(t *testing.T) *PollLog {
	t.Helper()
	return &PollLog{
		now:      time.Now,
		interval: pollLogInterval,
		print:    func(msg string) { t.Log(msg) },
	}
}

// StderrPollLog returns a PollLog that writes e2e/backend progress lines to
// stderr. Used from TestMain and fixture setup, when t.Log has nowhere to go.
func StderrPollLog() *PollLog {
	return &PollLog{
		now:      time.Now,
		interval: pollLogInterval,
		print:    printStderrProgress,
	}
}

// Logf writes a formatted wait-loop message, throttling identical repeats.
func (p *PollLog) Logf(format string, args ...any) {
	if p == nil {
		return
	}
	msg := fmt.Sprintf(format, args...)
	now := p.now()
	if p.lastMsg == msg && now.Sub(p.lastAt) < p.interval {
		return
	}
	p.lastMsg = msg
	p.lastAt = now
	p.print(msg)
}

// startStderrHeartbeat logs msg every commandHeartbeatInterval until the
// returned stop func is called. The first line is delayed by one interval so
// the caller can print a distinct start line immediately. stop is idempotent.
func startStderrHeartbeat(msg string) func() {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		log := &PollLog{
			now:      time.Now,
			interval: commandHeartbeatInterval,
			print:    printStderrProgress,
		}
		tick := time.NewTicker(commandHeartbeatInterval)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				log.Logf("%s", msg)
			}
		}
	}()
	return func() {
		once.Do(func() { close(done) })
	}
}

func printStderrProgress(msg string) {
	fmt.Fprintf(os.Stderr, "e2e/backend: %s\n", msg)
}
