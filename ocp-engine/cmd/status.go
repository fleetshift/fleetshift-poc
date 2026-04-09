package cmd

import (
	"os"

	"github.com/ocp-engine/internal/output"
	"github.com/ocp-engine/internal/phase"
	"github.com/ocp-engine/internal/workdir"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check the status of a work directory",
	Long:  "Inspects a work directory and reports on the state of cluster provisioning",
	RunE:  runStatus,
}

var statusWorkDir string

func init() {
	statusCmd.Flags().StringVar(&statusWorkDir, "work-dir", "", "Path to work directory (required)")
	statusCmd.MarkFlagRequired("work-dir")
	rootCmd.AddCommand(statusCmd)
}

// nextPhase returns the first phase not in the completed set
func nextPhase(completed []string) string {
	completedSet := make(map[string]bool)
	for _, p := range completed {
		completedSet[p] = true
	}

	for _, p := range phase.PhaseNames() {
		if !completedSet[p] {
			return p
		}
	}

	return "" // all phases complete
}

func runStatus(cmd *cobra.Command, args []string) error {
	// Step 1: Open work directory
	wd, err := workdir.Open(statusWorkDir)
	if err != nil {
		// Directory doesn't exist -- this is a valid "empty" state, not an error
		output.WriteStatusResult(os.Stdout, output.StatusResult{
			State:           "empty",
			CompletedPhases: []string{},
			HasKubeconfig:   false,
			HasMetadata:     false,
		})
		return nil
	}

	// Step 2: Get completed phases
	completed := wd.CompletedPhases()

	// Step 3: Read PID
	pid, pidAlive, _ := wd.ReadPID()

	// Step 4: Get infra ID (ignore error)
	infraID, _ := wd.InfraID()

	// Step 5: Determine state
	var state string
	hasKubeconfig := wd.HasKubeconfig()
	hasMetadata := wd.HasMetadata()

	if len(completed) == 0 && pid == 0 {
		state = "empty"
	} else if len(completed) == 5 && hasKubeconfig {
		state = "succeeded"
	} else if pid > 0 && pidAlive {
		state = "running"
	} else if pid > 0 && !pidAlive {
		state = "partial"
	} else {
		state = "failed"
	}

	// Step 6: Determine current phase
	currentPhase := nextPhase(completed)

	// Step 7: Write StatusResult
	output.WriteStatusResult(os.Stdout, output.StatusResult{
		State:           state,
		CompletedPhases: completed,
		CurrentPhase:    currentPhase,
		PID:             pid,
		PIDAlive:        pidAlive,
		InfraID:         infraID,
		HasKubeconfig:   hasKubeconfig,
		HasMetadata:     hasMetadata,
	})

	return nil
}
