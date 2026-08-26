package steps

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

// pollLog wraps harness.PollLog so step wait loops can keep calling logf.
type pollLog struct {
	*harness.PollLog
}

func newPollLog(t *testing.T) *pollLog {
	t.Helper()
	return &pollLog{harness.NewPollLog(t)}
}

func stderrPollLog() *pollLog {
	return &pollLog{harness.StderrPollLog()}
}

func (p *pollLog) logf(format string, args ...any) {
	if p == nil || p.PollLog == nil {
		return
	}
	p.Logf(format, args...)
}
