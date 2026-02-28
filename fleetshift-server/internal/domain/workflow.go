package domain

import "context"

// WorkflowStep is a durable step with a stable name, typed input, and
// typed output. Implementations must be safe for at-least-once invocation.
type WorkflowStep[I any, O any] interface {
	Name() string
	Run(ctx context.Context, in I) (O, error)
}

// WorkflowDefinition defines the orchestration of steps. Its [Run] method
// must be deterministic: no I/O, no randomness, no wall-clock reads. All
// side effects go through [WorkflowRun.Step].
type WorkflowDefinition[I any, O any] interface {
	Name() string
	Run(run WorkflowRun, in I) (O, error)
}

// WorkflowRun is the capability object provided to a running workflow.
// It provides the means to execute durable steps.
type WorkflowRun interface {
	RunID() string
	ExecuteStep(ctx context.Context, step WorkflowStep[any, any], in any) (any, error)
}

// RunStep is a convenience function that provides type safety when
// calling [WorkflowRun.ExecuteStep] with a typed [WorkflowStep].
func RunStep[I any, O any](run WorkflowRun, ctx context.Context, step WorkflowStep[I, O], in I) (O, error) {
	wrapper := &typedStepWrapper[I, O]{step: step}
	result, err := run.ExecuteStep(ctx, wrapper, in)
	if err != nil {
		var zero O
		return zero, err
	}
	return result.(O), nil
}

type typedStepWrapper[I any, O any] struct {
	step WorkflowStep[I, O]
}

func (w *typedStepWrapper[I, O]) Name() string { return w.step.Name() }
func (w *typedStepWrapper[I, O]) Run(ctx context.Context, in any) (any, error) {
	return w.step.Run(ctx, in.(I))
}

// WorkflowHandle is a handle to a running or completed workflow execution.
type WorkflowHandle[O any] interface {
	WorkflowID() string
	Get(ctx context.Context) (O, error)
}

// RegisteredWorkflow is a workflow that has been registered with an engine
// and can be invoked.
type RegisteredWorkflow[I any, O any] interface {
	Name() string
	Run(ctx context.Context, in I) (WorkflowHandle[O], error)
}

// WorkflowRegistry registers steps and workflows with a durable execution
// engine.
type WorkflowRegistry interface {
	RegisterWorkflow(wf WorkflowDefinition[any, any]) (RegisteredWorkflow[any, any], error)
}
