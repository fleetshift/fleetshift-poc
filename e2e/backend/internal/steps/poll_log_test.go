package steps

import "testing"

func TestPollLogWrappers(t *testing.T) {
	t.Parallel()
	t.Run("newPollLog", func(t *testing.T) {
		t.Parallel()
		p := newPollLog(t)
		if p == nil || p.PollLog == nil {
			t.Fatal("newPollLog returned nil")
		}
		p.logf("ok")
	})
	t.Run("stderrPollLog", func(t *testing.T) {
		t.Parallel()
		p := stderrPollLog()
		if p == nil || p.PollLog == nil {
			t.Fatal("stderrPollLog returned nil")
		}
	})
	t.Run("nil logf", func(t *testing.T) {
		t.Parallel()
		var p *pollLog
		p.logf("ignored")
		p = &pollLog{}
		p.logf("ignored")
	})
}
