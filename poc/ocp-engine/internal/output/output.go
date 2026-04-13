package output

import (
	"encoding/json"
	"io"
)

type PhaseResult struct {
	Phase           string `json:"phase"`
	Status          string `json:"status"`
	Error           string `json:"error,omitempty"`
	LogTail         string `json:"log_tail,omitempty"`
	ElapsedSeconds  int    `json:"elapsed_seconds"`
	RequiresDestroy bool   `json:"requires_destroy,omitempty"`
	Attempt         int    `json:"attempt,omitempty"`
}

type ErrorResult struct {
	Category        string `json:"category"`
	Phase           string `json:"phase,omitempty"`
	Message         string `json:"message"`
	LogTail         string `json:"log_tail,omitempty"`
	HasMetadata     bool   `json:"has_metadata,omitempty"`
	RequiresDestroy bool   `json:"requires_destroy"`
	Attempt         int    `json:"attempt,omitempty"`
}

type StatusResult struct {
	State           string   `json:"state"`
	CompletedPhases []string `json:"completed_phases"`
	CurrentPhase    string   `json:"current_phase,omitempty"`
	PID             int      `json:"pid,omitempty"`
	PIDAlive        bool     `json:"pid_alive,omitempty"`
	InfraID         string   `json:"infra_id,omitempty"`
	HasKubeconfig   bool     `json:"has_kubeconfig"`
	HasMetadata     bool     `json:"has_metadata"`
	Error           string   `json:"error,omitempty"`
}

type PreflightResult struct {
	Phase          string `json:"phase"`
	Status         string `json:"status"`
	AWSAccount     string `json:"aws_account,omitempty"`
	AWSARN         string `json:"aws_arn,omitempty"`
	DNSWarning     string `json:"dns_warning,omitempty"`
	ElapsedSeconds int    `json:"elapsed_seconds"`
	Attempt        int    `json:"attempt,omitempty"`
}

type ProvisionResult struct {
	Status  string `json:"status"`
	InfraID string `json:"infra_id,omitempty"`
	Attempt int    `json:"attempt,omitempty"`
}

type DestroyResult struct {
	Action         string `json:"action"`
	Status         string `json:"status"`
	InfraID        string `json:"infra_id,omitempty"`
	Error          string `json:"error,omitempty"`
	LogTail        string `json:"log_tail,omitempty"`
	ElapsedSeconds int    `json:"elapsed_seconds"`
}

type MilestoneEvent struct {
	Event          string `json:"event"`
	ElapsedSeconds int    `json:"elapsed_seconds"`
	Attempt        int    `json:"attempt,omitempty"`
}

func writeJSON(w io.Writer, v any) {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(v)
}

func WritePhaseResult(w io.Writer, r PhaseResult) {
	writeJSON(w, r)
}

func WriteErrorResult(w io.Writer, r ErrorResult) {
	writeJSON(w, r)
}

// WriteError is a convenience wrapper that writes an ErrorResult and returns the error.
func WriteError(w io.Writer, category string, err error, requiresDestroy bool) error {
	WriteErrorResult(w, ErrorResult{
		Category:        category,
		Message:         err.Error(),
		RequiresDestroy: requiresDestroy,
	})
	return err
}

func WritePreflightResult(w io.Writer, r PreflightResult) {
	writeJSON(w, r)
}

func WriteStatusResult(w io.Writer, r StatusResult) {
	writeJSON(w, r)
}

func WriteProvisionResult(w io.Writer, r ProvisionResult) {
	writeJSON(w, r)
}

func WriteDestroyResult(w io.Writer, r DestroyResult) {
	writeJSON(w, r)
}

func WriteMilestoneEvent(w io.Writer, e MilestoneEvent) {
	writeJSON(w, e)
}
