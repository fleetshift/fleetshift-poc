package workdir

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// allPhases defines the canonical order of OpenShift installation phases
var allPhases = []string{"extract", "install-config", "manifests", "ignition", "cluster"}

// WorkDir represents a cluster-specific working directory
type WorkDir struct {
	Path string
}

// Init creates a new work directory (or opens an existing one)
func Init(path string) (*WorkDir, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("create work-dir: %w", err)
	}
	return &WorkDir{Path: path}, nil
}

// Open opens an existing work directory
func Open(path string) (*WorkDir, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("work-dir does not exist: %s", path)
	}
	return &WorkDir{Path: path}, nil
}

// Lock writes a PID file to claim exclusive access to the work directory
func (w *WorkDir) Lock() error {
	pidFile := filepath.Join(w.Path, ".pid")

	// Check if already locked
	if pid, alive, err := w.ReadPID(); err == nil {
		if alive {
			return fmt.Errorf("work-dir is locked by PID %d", pid)
		}
		// PID file exists but process is dead, clean it up
		os.Remove(pidFile)
	}

	// Write our PID
	pid := os.Getpid()
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(pid)), 0644); err != nil {
		return fmt.Errorf("write PID file: %w", err)
	}

	return nil
}

// Unlock removes the PID file to release the lock
func (w *WorkDir) Unlock() error {
	pidFile := filepath.Join(w.Path, ".pid")
	if err := os.Remove(pidFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove PID file: %w", err)
	}
	return nil
}

// ReadPID reads the PID from the lock file and checks if the process is alive
func (w *WorkDir) ReadPID() (int, bool, error) {
	pidFile := filepath.Join(w.Path, ".pid")
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, false, err
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, false, fmt.Errorf("parse PID: %w", err)
	}

	// Check if process is alive by sending signal 0
	process, err := os.FindProcess(pid)
	if err != nil {
		return pid, false, nil
	}

	err = process.Signal(syscall.Signal(0))
	alive := err == nil

	return pid, alive, nil
}

// HasMetadata checks if metadata.json exists
func (w *WorkDir) HasMetadata() bool {
	_, err := os.Stat(filepath.Join(w.Path, "metadata.json"))
	return err == nil
}

// HasKubeconfig checks if auth/kubeconfig exists
func (w *WorkDir) HasKubeconfig() bool {
	_, err := os.Stat(filepath.Join(w.Path, "auth", "kubeconfig"))
	return err == nil
}

// HasInstaller checks if openshift-install binary exists
func (w *WorkDir) HasInstaller() bool {
	_, err := os.Stat(filepath.Join(w.Path, "openshift-install"))
	return err == nil
}

// InfraID reads the infraID from metadata.json
func (w *WorkDir) InfraID() (string, error) {
	metadataPath := filepath.Join(w.Path, "metadata.json")
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return "", fmt.Errorf("read metadata.json: %w", err)
	}

	var metadata struct {
		InfraID string `json:"infraID"`
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return "", fmt.Errorf("parse metadata.json: %w", err)
	}

	return metadata.InfraID, nil
}

// InstallerPath returns the path to the openshift-install binary
func (w *WorkDir) InstallerPath() string {
	return filepath.Join(w.Path, "openshift-install")
}

// InstallConfigPath returns the path to install-config.yaml
func (w *WorkDir) InstallConfigPath() string {
	return filepath.Join(w.Path, "install-config.yaml")
}

// LogPath returns the path to the installation log file
func (w *WorkDir) LogPath() string {
	return filepath.Join(w.Path, ".openshift_install.log")
}

// MarkPhaseComplete creates a marker file indicating the phase has completed
func (w *WorkDir) MarkPhaseComplete(phase string) error {
	markerPath := filepath.Join(w.Path, "_phase_"+phase+"_complete")
	if err := os.WriteFile(markerPath, []byte(""), 0644); err != nil {
		return fmt.Errorf("write phase marker: %w", err)
	}
	return nil
}

// IsPhaseComplete checks if a phase marker file exists
func (w *WorkDir) IsPhaseComplete(phase string) bool {
	markerPath := filepath.Join(w.Path, "_phase_"+phase+"_complete")
	_, err := os.Stat(markerPath)
	return err == nil
}

// CompletedPhases returns an ordered list of completed phases
func (w *WorkDir) CompletedPhases() []string {
	var completed []string
	for _, phase := range allPhases {
		if w.IsPhaseComplete(phase) {
			completed = append(completed, phase)
		}
	}
	return completed
}

// LogTail returns the last N lines of the installation log file
func (w *WorkDir) LogTail(lines int) string {
	logPath := w.LogPath()
	data, err := os.ReadFile(logPath)
	if err != nil {
		return ""
	}

	allLines := strings.Split(string(data), "\n")
	if len(allLines) <= lines {
		return string(data)
	}

	start := len(allLines) - lines
	return strings.Join(allLines[start:], "\n")
}
