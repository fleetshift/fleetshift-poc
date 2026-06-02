// Package testutil provides shared test-support utilities.
package testutil

import "time"

// Standard test timeout tiers. Pick the tier that matches the
// workload under test rather than guessing a raw duration. See the
// table below for guidance:
//
//	Tier                Duration  When to use
//	UnitTimeout         5 s       Sync stubs, single operation, no async dispatch
//	ServiceTimeout      15 s      Async memworkflow, full lifecycle (create→active→delete→gone)
//	ContractTimeout     15 s      Workflow-engine contract tests, standard subtests
//	RetryTimeout        30 s      Multi-retry / ContinueAsNew paths
//	IntegrationTimeout  5 min     Real Docker / kind clusters (skipped with -short)
const (
	UnitTimeout        = 5 * time.Second
	ServiceTimeout     = 15 * time.Second
	ContractTimeout    = 15 * time.Second
	RetryTimeout       = 30 * time.Second
	IntegrationTimeout = 5 * time.Minute
)
