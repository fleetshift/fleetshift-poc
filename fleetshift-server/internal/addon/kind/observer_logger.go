package kind

import (
	"fmt"
	"time"

	"sigs.k8s.io/kind/pkg/log"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// observerLogger adapts a [domain.DeliveryObserver] into a kind
// [log.Logger]. V(0) messages (user-facing progress) become progress
// events; warnings and errors map to their respective event kinds.
// Higher verbosity levels (V(1+)) are discarded.
type observerLogger struct {
	observer domain.DeliveryObserver
	now      func() time.Time
}

// NewObserverLogger creates a kind [log.Logger] that forwards messages
// to the given [domain.DeliveryObserver].
func NewObserverLogger(observer domain.DeliveryObserver, now func() time.Time) log.Logger {
	if now == nil {
		now = time.Now
	}
	return &observerLogger{observer: observer, now: now}
}

func (l *observerLogger) Warn(message string) {
	l.observer.Emit(domain.DeliveryEvent{
		Timestamp: l.now(),
		Kind:      domain.DeliveryEventWarning,
		Message:   message,
	})
}

func (l *observerLogger) Warnf(format string, args ...interface{}) {
	l.Warn(fmt.Sprintf(format, args...))
}

func (l *observerLogger) Error(message string) {
	l.observer.Emit(domain.DeliveryEvent{
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
	return &observerInfoLogger{observer: l.observer, now: l.now}
}

// observerInfoLogger emits progress events for V(0) messages.
type observerInfoLogger struct {
	observer domain.DeliveryObserver
	now      func() time.Time
}

func (l *observerInfoLogger) Info(message string) {
	l.observer.Emit(domain.DeliveryEvent{
		Timestamp: l.now(),
		Kind:      domain.DeliveryEventProgress,
		Message:   message,
	})
}

func (l *observerInfoLogger) Infof(format string, args ...interface{}) {
	l.Info(fmt.Sprintf(format, args...))
}

func (l *observerInfoLogger) Enabled() bool { return true }
