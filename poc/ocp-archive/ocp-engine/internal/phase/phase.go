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

// PhaseNames returns just the phase name strings in canonical order.
func PhaseNames() []string {
	phases := AllPhases()
	names := make([]string, len(phases))
	for i, p := range phases {
		names[i] = p.Name
	}
	return names
}

func AllPhases() []Phase {
	return []Phase{
		{Name: "preflight", RequiresDestroyOnFailure: false},
		{Name: "extract", RequiresDestroyOnFailure: false},
		{Name: "ccoctl", RequiresDestroyOnFailure: false},
		{Name: "install-config", RequiresDestroyOnFailure: false},
		{Name: "manifests", RequiresDestroyOnFailure: false},
		{Name: "ignition", RequiresDestroyOnFailure: false},
		{Name: "cluster", RequiresDestroyOnFailure: true},
	}
}

func RunPhase(p Phase, fn func() error, w io.Writer, attempt int) error {
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
			Attempt:         attempt,
		})
		return err
	}
	output.WritePhaseResult(w, output.PhaseResult{
		Phase:          p.Name,
		Status:         "complete",
		ElapsedSeconds: elapsed,
		Attempt:        attempt,
	})
	return nil
}
