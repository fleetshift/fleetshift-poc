package phase

import (
	"io"
	"time"

	"github.com/ocp-engine/internal/output"
)

type Phase struct {
	Name                     string
	RequiresDestroyOnFailure bool
}

func AllPhases() []Phase {
	return []Phase{
		{Name: "extract", RequiresDestroyOnFailure: false},
		{Name: "install-config", RequiresDestroyOnFailure: false},
		{Name: "manifests", RequiresDestroyOnFailure: false},
		{Name: "ignition", RequiresDestroyOnFailure: false},
		{Name: "cluster", RequiresDestroyOnFailure: true},
	}
}

func RunPhase(p Phase, fn func() error, w io.Writer) error {
	start := time.Now()
	err := fn()
	elapsed := int(time.Since(start).Seconds())
	if err != nil {
		output.WritePhaseResult(w, output.PhaseResult{
			Phase:           p.Name,
			Status:          "failed",
			Error:           err.Error(),
			ElapsedSeconds:  elapsed,
			RequiresDestroy: p.RequiresDestroyOnFailure,
		})
		return err
	}
	output.WritePhaseResult(w, output.PhaseResult{
		Phase:          p.Name,
		Status:         "complete",
		ElapsedSeconds: elapsed,
	})
	return nil
}
