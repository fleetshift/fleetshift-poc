package domain

import "context"

// ImmediateRollout emits one remove step (all removed targets) then one deliver
// step (all added and unchanged targets). No gates; removals and deliveries
// run immediately.
type ImmediateRollout struct{}

func (r *ImmediateRollout) Plan(_ context.Context, delta TargetDelta) (RolloutPlan, error) {
	var steps []RolloutStep

	if len(delta.Removed) > 0 {
		steps = append(steps, RolloutStep{
			Remove: &RolloutStepRemove{Targets: delta.Removed},
		})
	}

	deliverTargets := make([]TargetInfo, 0, len(delta.Added)+len(delta.Unchanged))
	deliverTargets = append(deliverTargets, delta.Added...)
	deliverTargets = append(deliverTargets, delta.Unchanged...)
	if len(deliverTargets) > 0 {
		steps = append(steps, RolloutStep{
			Deliver: &RolloutStepDeliver{Targets: deliverTargets},
		})
	}

	return RolloutPlan{Steps: steps}, nil
}
