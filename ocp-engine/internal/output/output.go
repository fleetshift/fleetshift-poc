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
}

type ErrorResult struct {
	Category        string `json:"category"`
	Phase           string `json:"phase,omitempty"`
	Message         string `json:"message"`
	LogTail         string `json:"log_tail,omitempty"`
	HasMetadata     bool   `json:"has_metadata,omitempty"`
	RequiresDestroy bool   `json:"requires_destroy"`
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

type ProvisionResult struct {
	Status  string `json:"status"`
	InfraID string `json:"infra_id,omitempty"`
}

type DestroyResult struct {
	Action         string `json:"action"`
	Status         string `json:"status"`
	InfraID        string `json:"infra_id,omitempty"`
	Error          string `json:"error,omitempty"`
	LogTail        string `json:"log_tail,omitempty"`
	ElapsedSeconds int    `json:"elapsed_seconds"`
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

func WriteStatusResult(w io.Writer, r StatusResult) {
	writeJSON(w, r)
}

func WriteProvisionResult(w io.Writer, r ProvisionResult) {
	writeJSON(w, r)
}

func WriteDestroyResult(w io.Writer, r DestroyResult) {
	writeJSON(w, r)
}
