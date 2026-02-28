package domain

import "context"

// ImmediateRollout places all targets into a single batch with no gates.
type ImmediateRollout struct{}

func (r *ImmediateRollout) Plan(_ context.Context, delta TargetDelta) (RolloutPlan, error) {
	targets := make([]TargetInfo, 0, len(delta.Added)+len(delta.Unchanged))
	targets = append(targets, delta.Added...)
	targets = append(targets, delta.Unchanged...)

	if len(targets) == 0 {
		return RolloutPlan{}, nil
	}

	return RolloutPlan{
		Batches: []RolloutBatch{{Targets: targets}},
	}, nil
}
