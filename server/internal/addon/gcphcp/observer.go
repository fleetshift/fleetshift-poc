package gcphcp

import "log/slog"

// AgentObserver provides structured logging for the agent.
type AgentObserver interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

// SlogAgentObserver implements AgentObserver using slog.
type SlogAgentObserver struct {
	Logger *slog.Logger
}

// NewSlogAgentObserver creates a new slog-based observer with addon label.
func NewSlogAgentObserver(logger *slog.Logger) *SlogAgentObserver {
	return &SlogAgentObserver{Logger: logger.With("addon", "gcphcp")}
}

// Info logs an informational message.
func (o *SlogAgentObserver) Info(msg string, args ...any) {
	o.Logger.Info(msg, args...)
}

// Error logs an error message.
func (o *SlogAgentObserver) Error(msg string, args ...any) {
	o.Logger.Error(msg, args...)
}

// noopObserver implements AgentObserver with no-op methods.
type noopObserver struct{}

// Info is a no-op.
func (noopObserver) Info(string, ...any) {}

// Error is a no-op.
func (noopObserver) Error(string, ...any) {}
