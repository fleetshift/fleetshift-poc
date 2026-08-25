package steps

import (
	"fmt"
	"testing"
	"time"
)

// pollLogInterval is how often identical wait-loop messages are logged.
const pollLogInterval = 3 * time.Second

// pollLog writes throttled log lines during wait loops. Identical messages
// are withheld until interval elapses; a different message is always logged.
type pollLog struct {
	now      func() time.Time
	interval time.Duration
	lastAt   time.Time
	lastMsg  string
	print    func(string)
}

// newPollLog returns a pollLog that writes to t.Log, throttling identical
// messages to pollLogInterval.
func newPollLog(t *testing.T) *pollLog {
	t.Helper()
	return &pollLog{
		now:      time.Now,
		interval: pollLogInterval,
		print:    func(msg string) { t.Log(msg) },
	}
}

// logf writes a formatted wait-loop message, throttling identical repeats.
func (p *pollLog) logf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	now := p.now()
	if p.lastMsg == msg && now.Sub(p.lastAt) < p.interval {
		return
	}
	p.lastMsg = msg
	p.lastAt = now
	p.print(msg)
}
