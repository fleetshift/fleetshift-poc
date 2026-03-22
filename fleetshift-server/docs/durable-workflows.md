Goals

Design an application-facing abstraction where:

- The application defines:
  - workflow bodies (orchestration)
  - activity bodies (business logic + side effects)
  - activity boundaries (fine grained)
- The application does not import go-workflows or DBOS.
- Infrastructure can provide any of the three implementations:
  - go-workflows workflows + activities
  - DBOS workflows + RunAsStep steps
  - memworkflow (in-memory, for fast tests)

⸻

Core design rule

Do not make activities closures

To be portable, an activity must be a stable identity + explicit input, not:
	•	RunActivity(record, func() {...}) (closure capturing values)

Instead use:
	•	Activity[I,O]{Name, Run(ctx, input)} (explicit input, stable identity)

This matches how go-workflows (and Temporal-like systems) record work: "activity function + serialized args."

DBOS executes the activity by calling RunAsStep and invoking activity.Run(ctx, input) inside it.

⸻

The port surface (application-visible)

These types live in internal/domain/workflow.go:

1. Activity definition

Has a stable name, typed input/output, and runs with context.Context (activity side always does).

```go
type Activity[I any, O any] interface {
  Name() string
  Run(ctx context.Context, in I) (O, error)
}
```

Activities are created with the NewActivity helper:

```go
func NewActivity[I, O any](name string, fn func(context.Context, I) (O, error)) Activity[I, O]
```

1. Workflow spec

A concrete struct per workflow, holding dependencies and providing:
	•	Name() string
	•	Run(record Record, in I) (O, error) — deterministic orchestration body
	•	Activity methods — each returns a typed Activity[I,O] that closes over the spec's dependencies

```go
type OrchestrationWorkflowSpec struct {
  Store      Store
  Delivery   DeliveryService
  Strategies StrategyFactory
  Registry   Registry
  // ... other dependencies
}

func (s *OrchestrationWorkflowSpec) Name() string { return "orchestrate-deployment" }
func (s *OrchestrationWorkflowSpec) Run(record Record, deploymentID DeploymentID) (struct{}, error)

// Activity methods — each returns a typed Activity
func (s *OrchestrationWorkflowSpec) LoadDeploymentAndPool() Activity[DeploymentID, DeploymentAndPool]
func (s *OrchestrationWorkflowSpec) ResolvePlacement() Activity[ResolvePlacementInput, []PlacementTarget]
// ...
```

1. Durable execution record

Provided by the engine at runtime. Records activity invocations and signal awaits for deterministic replay.

```go
type Record interface {
  ID() string
  Context() context.Context
  Run(activity Activity[any, any], in any) (any, error)
  Await(signalName string) (any, error)
}
```

Application code never calls Record.Run or Record.Await directly. Instead, use the typed helper functions:

```go
func RunActivity[I, O any](record Record, activity Activity[I, O], in I) (O, error)
func AwaitSignal[T any](record Record, sig Signal[T]) (T, error)
```

1. Signals

Named, typed channels for cross-workflow communication. A Signal value is shared between the send side (Registry.SignalDeploymentEvent) and the receive side (AwaitSignal).

```go
type Signal[T any] struct {
  Name string
}

var DeploymentEventSignal = Signal[DeploymentEvent]{Name: "deployment-event"}
```

1. Registration + invocation handles

The Registry registers workflow specs and returns per-workflow interfaces that can start instances. Each workflow type has its own registration method and returned handle type.

```go
type Registry interface {
  RegisterOrchestration(spec *OrchestrationWorkflowSpec) (OrchestrationWorkflow, error)
  RegisterCreateDeployment(spec *CreateDeploymentWorkflowSpec) (CreateDeploymentWorkflow, error)
  SignalDeploymentEvent(ctx context.Context, deploymentID DeploymentID, event DeploymentEvent) error
}

type OrchestrationWorkflow interface {
  Start(ctx context.Context, deploymentID DeploymentID) (Execution[struct{}], error)
}

type CreateDeploymentWorkflow interface {
  Start(ctx context.Context, input CreateDeploymentInput) (Execution[Deployment], error)
}

type Execution[T any] interface {
  WorkflowID() string
  AwaitResult(ctx context.Context) (T, error)
}
```

Keep the port small. Add features only when you need them (timeouts, retry hints, cancellation).

⸻

Application structure

Application service responsibilities
	•	Construct the workflow spec structs with their dependencies.
	•	Call Registry.RegisterOrchestration / RegisterCreateDeployment during initialization.
	•	Store the returned OrchestrationWorkflow / CreateDeploymentWorkflow handle.
	•	Business-facing methods call handle.Start(...) and optionally execution.AwaitResult(...).

Where logic lives
	•	Activity bodies: in activity methods on the workflow spec (domain logic, repo calls, external effects).
	•	Orchestration: in the spec's Run method (the ordering/branching of activities via RunActivity and AwaitSignal).
	•	No go-workflows/DBOS imports anywhere in domain packages.

⸻

Backend mapping rules

go-workflows implementation (internal/infrastructure/goworkflows)
	•	RegisterOrchestration(spec):
	•	For each activity method on the spec, call registerActivity which:
	•	registers a wrapper function with Worker.RegisterActivity(fn, WithName(name))
	•	creates a typed invoker that calls workflow.ExecuteActivity[O](wfCtx, workflow.DefaultActivityOptions, name, in).Get(wfCtx)
	•	Register a wrapper workflow function with Worker.RegisterWorkflow(fn, WithName(spec.Name())). The wrapper:
	•	creates signal channels with workflow.NewSignalChannel[T](ctx, signalName)
	•	builds a baseRecord with the workflow context, invokers map, and signal receivers
	•	calls spec.Run(record, input)
	•	Return a handle whose Start method calls client.CreateWorkflowInstance.
	•	Record.Run(activity, input):
	•	looks up the invoker by activity.Name() and calls it, which executes workflow.ExecuteActivity under the hood
	•	Record.Await(signalName):
	•	calls the pre-registered signal receiver, which calls ch.Receive(ctx)
	•	Execution.AwaitResult:
	•	calls client.GetWorkflowResult[O](ctx, client, instance, timeout)
	•	SignalDeploymentEvent:
	•	calls client.SignalWorkflow(ctx, instanceID, signalName, event)

DBOS implementation (internal/infrastructure/dbosworkflows)
	•	RegisterOrchestration(spec):
	•	For each activity method, create a typed invoker that calls dbos.RunAsStep(ctx, func(stepCtx) (O, error) { return activity.Run(stepCtx, in.(I)) }, dbos.WithStepName(activity.Name()))
	•	Register a wrapper workflow function with dbos.RegisterWorkflow(dbosCtx, fn, dbos.WithWorkflowName(spec.Name())). The wrapper:
	•	builds a baseRecord with the DBOS context, invokers map, and signal receivers
	•	signal receivers use dbos.Recv[T](ctx, signalName, timeout) in a loop, skipping zero-value events
	•	calls spec.Run(record, input)
	•	Return a handle whose Start method calls dbos.RunWorkflow.
	•	Execution.AwaitResult:
	•	calls handle.GetResult()
	•	SignalDeploymentEvent:
	•	calls dbos.Send(dbosCtx, workflowID, event, signalName)
	•	dbos.Launch:
	•	called once via sync.Once on first RegisterCreateDeployment call

memworkflow implementation (internal/infrastructure/memworkflow)
	•	No activity registration needed.
	•	RegisterOrchestration(spec):
	•	stores the spec; Start dispatches spec.Run in a goroutine with a baseRecord
	•	Record.Run(activity, input):
	•	JSON round-trips input, dispatches activity.Run(context.Background(), in) in a goroutine, JSON round-trips the output. This catches serialization issues that would be silent without a durable engine.
	•	Record.Await(signalName):
	•	blocks on a buffered channel of JSON-serialized events, deserializes on receive
	•	SignalDeploymentEvent:
	•	JSON-serializes the event and sends it on the instance's channel, mirroring how durable engines persist signals before delivering them
	•	No durable state or replay. Recommended workflow backend for fast, high-fidelity tests.

Important: workflow code must remain deterministic across engines. Orchestration should not call time/random/network directly; put those behind activities.

⸻

Determinism and idempotency guidance (LLM should enforce)
	•	Workflow body (Spec.Run) should:
	•	only orchestrate activities via RunActivity, await signals via AwaitSignal, branch on returned values, and manipulate pure data
	•	avoid direct side effects
	•	Activity bodies (Activity.Run) may do side effects but must be safe under retries:
	•	add idempotency keys or "already done" guards for external calls (payments, email, publish)
	•	database activities should be bounded transactions

⸻

Quality checks ("litmus tests")

An abstraction is "portable" if:
	•	Activities are explicit defs with stable identity (Name()) + typed inputs (no closures).
	•	Workflow orchestration is framework-free and only uses RunActivity and AwaitSignal.
	•	You can implement Record.Run using:
	•	go-workflows: workflow.ExecuteActivity[O](wfCtx, options, name, in).Get(wfCtx)
	•	DBOS: dbos.RunAsStep(ctx, func(stepCtx) (O, error) { return activity.Run(stepCtx, in) }, dbos.WithStepName(name))
	•	memworkflow: JSON round-trip + goroutine dispatch

It's "clean" if:
	•	domain packages compile with zero go-workflows/DBOS imports
	•	infrastructure is the only place that imports those libraries

⸻

What to generate when asked for code

When asked to implement a workflow:
	•	define Activity methods on the workflow spec for each durable boundary, using NewActivity(name, func)
	•	define the spec's Run method orchestrating them with RunActivity(record, spec.SomeActivity(), input)
	•	use AwaitSignal(record, SomeSignal) for cross-workflow signaling
	•	in the service constructor: register the spec via Registry and store the returned handle
	•	in service method: invoke handle.Start(...) and optionally execution.AwaitResult(...)

When asked to implement a new backend:
	•	implement Registry, the per-workflow interfaces, and Execution[T]
	•	implement Record with Run and Await
	•	map Record.Run to the engine's durable primitive:
	•	go-workflows: ExecuteActivity
	•	DBOS: RunAsStep
	•	memworkflow: goroutine + JSON round-trip
	•	map Record.Await to the engine's signal primitive:
	•	go-workflows: workflow.NewSignalChannel + Receive
	•	DBOS: dbos.Recv (loop, skip zero-value)
	•	memworkflow: buffered channel + JSON deserialize