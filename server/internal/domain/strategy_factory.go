package domain

import "fmt"

// StrategyFactory creates strategy implementations from user-provided
// specs. Holds dependencies that individual strategies may need (e.g.
// a [Store] for resolving managed-resource intents).
type StrategyFactory struct {
	Store Store
}

func (f StrategyFactory) ManifestStrategy(spec ManifestStrategySpec) (ManifestStrategy, error) {
	switch spec.Type {
	case ManifestStrategyInline:
		return &InlineManifestStrategy{Manifests: spec.Manifests}, nil
	case ManifestStrategyManagedResource:
		return &ManagedResourceManifestStrategy{
			Ref:   spec.IntentRef,
			Store: f.Store,
		}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported manifest strategy type %q", ErrInvalidArgument, spec.Type)
	}
}

func (f StrategyFactory) PlacementStrategy(spec PlacementStrategySpec) (PlacementStrategy, error) {
	switch spec.Type {
	case PlacementStrategyStatic:
		return &StaticPlacement{Targets: spec.Targets}, nil
	case PlacementStrategyAll:
		return &AllPlacement{}, nil
	case PlacementStrategySelector:
		if spec.TargetSelector == nil {
			return nil, fmt.Errorf("%w: selector placement requires a target selector", ErrInvalidArgument)
		}
		return &SelectorPlacement{Selector: *spec.TargetSelector}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported placement strategy type %q", ErrInvalidArgument, spec.Type)
	}
}

func (f StrategyFactory) RolloutStrategy(spec *RolloutStrategySpec) RolloutStrategy {
	if spec == nil {
		return &ImmediateRollout{}
	}
	switch spec.Type {
	case RolloutStrategyImmediate:
		return &ImmediateRollout{}
	default:
		return &ImmediateRollout{}
	}
}
