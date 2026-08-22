package steps

import (
	"strings"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

const fleetctlDetailLimit = 4096

// fleetctlDetail is stderr then stdout for Gomega/t.Fatal messages.
func fleetctlDetail(res harness.FleetctlResult) string {
	var b strings.Builder
	if s := strings.TrimSpace(res.Stderr); s != "" {
		b.WriteString("stderr=")
		b.WriteString(clip(s, fleetctlDetailLimit))
	}
	if s := strings.TrimSpace(res.Stdout); s != "" {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString("stdout=")
		b.WriteString(clip(s, fleetctlDetailLimit))
	}
	return b.String()
}

// clip returns s, or a truncated prefix when s is longer than n bytes.
func clip(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
