package kind

import (
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/kind/pkg/log"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// observerLogger adapts a [domain.DeliverySignaler] into a kind
// [log.Logger]. V(0) messages (user-facing progress) become progress
// events; warnings and errors map to their respective event kinds.
// Higher verbosity levels (V(1+)) are discarded.
//
// The kind [log.Logger] interface does not carry context, so the
// delivery context is captured at construction time.
type observerLogger struct {
	ctx      context.Context
	signaler *domain.DeliverySignaler
	now      func() time.Time
}

// NewObserverLogger creates a kind [log.Logger] that forwards messages
// to the given [domain.DeliverySignaler]. The provided ctx is used for
// all signaler calls since the kind logger interface has no context.
func NewObserverLogger(ctx context.Context, signaler *domain.DeliverySignaler, now func() time.Time) log.Logger {
	if now == nil {
		now = time.Now
	}
	return &observerLogger{ctx: ctx, signaler: signaler, now: now}
}

func (l *observerLogger) Warn(message string) {
	l.signaler.Emit(l.ctx, domain.DeliveryEvent{
		Timestamp: l.now(),
		Kind:      domain.DeliveryEventWarning,
		Message:   message,
	})
}

func (l *observerLogger) Warnf(format string, args ...interface{}) {
	l.Warn(fmt.Sprintf(format, args...))
}

func (l *observerLogger) Error(message string) {
	l.signaler.Emit(l.ctx, domain.DeliveryEvent{
		Timestamp: l.now(),
		Kind:      domain.DeliveryEventError,
		Message:   message,
	})
}

func (l *observerLogger) Errorf(format string, args ...interface{}) {
	l.Error(fmt.Sprintf(format, args...))
}

func (l *observerLogger) V(level log.Level) log.InfoLogger {
	if level > 0 {
		return log.NoopInfoLogger{}
	}
	return &observerInfoLogger{ctx: l.ctx, signaler: l.signaler, now: l.now}
}

// observerInfoLogger emits progress events for V(0) messages.
type observerInfoLogger struct {
	ctx      context.Context
	signaler *domain.DeliverySignaler
	now      func() time.Time
}

func (l *observerInfoLogger) Info(message string) {
	l.signaler.Emit(l.ctx, domain.DeliveryEvent{
		Timestamp: l.now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   message,
	})
}

func (l *observerInfoLogger) Infof(format string, args ...interface{}) {
	l.Info(fmt.Sprintf(format, args...))
}

func (l *observerInfoLogger) Enabled() bool { return true }
