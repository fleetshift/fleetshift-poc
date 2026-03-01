package domain

import "fmt"

// StrategyFactory instantiates the appropriate strategy implementation
// from a user-provided spec.
type StrategyFactory interface {
	ManifestStrategy(spec ManifestStrategySpec) (ManifestStrategy, error)
	PlacementStrategy(spec PlacementStrategySpec) (PlacementStrategy, error)
	RolloutStrategy(spec *RolloutStrategySpec) RolloutStrategy
}

// DefaultStrategyFactory creates built-in strategy implementations.
// Built-in strategies are currently pure with no I/O; the orchestration
// pipeline invokes all strategies from activities so that custom or
// future strategies may perform I/O or stateful behavior safely.
type DefaultStrategyFactory struct{}

func (f DefaultStrategyFactory) ManifestStrategy(spec ManifestStrategySpec) (ManifestStrategy, error) {
	switch spec.Type {
	case ManifestStrategyInline:
		return &InlineManifestStrategy{Manifests: spec.Manifests}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported manifest strategy type %q", ErrInvalidArgument, spec.Type)
	}
}

func (f DefaultStrategyFactory) PlacementStrategy(spec PlacementStrategySpec) (PlacementStrategy, error) {
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

func (f DefaultStrategyFactory) RolloutStrategy(spec *RolloutStrategySpec) RolloutStrategy {
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
