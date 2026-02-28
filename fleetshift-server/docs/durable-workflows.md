Goals

Design an application-facing abstraction where:
	•	The application defines:
	•	workflow bodies (orchestration)
	•	step bodies (business logic + side effects)
	•	step boundaries (fine grained)
	•	The application does not import DBOS or go-workflows.
	•	Infrastructure can provide either implementation:
	•	DBOS workflows + RunAsStep steps
	•	go-workflows workflows + activities

⸻

Core design rule

Do not make “steps” closures

To be portable, a step must be a stable identity + explicit input, not:
	•	Step(name, func() {...}) (closure capturing values)

Instead use:
	•	StepDef{Name, Run(ctx, input)} (explicit input, stable identity)

This matches how go-workflows (and Temporal-like systems) record work: “activity function + serialized args.”

DBOS can still execute the step by calling RunAsStep and invoking step.Run(ctx, input) inside it.

⸻

The port surface (application-visible)

Implement these DBOS/go-workflows-neutral types:
	1.	Step definition

	•	Has a stable name
	•	Has typed input/output
	•	Runs with context.Context (activity side always does)

type Step[I any, O any] interface {
  Name() string
  Run(ctx context.Context, in I) (O, error)
}

	2.	Workflow definition

	•	Has a stable name
	•	Orchestrates steps via a Run capability object
	•	Does not depend on framework context types

type Workflow[I any, O any] interface {
  Name() string
  Run(run Run, in I) (O, error)
}

	3.	Workflow execution capability (the “Tx-like object”)

	•	Provided by the engine at runtime
	•	Runs steps durably

type Run interface {
  RunID() string
  Step[I any, O any](step Step[I,O], in I) (O, error)
}

	4.	Registration + invocation handles

	•	App registers workflows/steps at init and stores handles
	•	App methods “call workflows like normal”

type Registry interface {
  RegisterStep[I any, O any](st Step[I,O]) error
  RegisterWorkflow[I any, O any](wf Workflow[I,O]) (RegisteredWorkflow[I,O], error)
}

type RegisteredWorkflow[I any, O any] interface {
  Name() string
  Run(ctx context.Context, in I) (Handle[O], error)
}

type Handle[O any] interface {
  WorkflowID() string
  GetResult(ctx context.Context) (O, error)
}

Keep the port small. Add features only when you need them (timeouts, retry hints, signals, cancellation).

⸻

Application structure

Application service responsibilities
	•	Construct step defs.
	•	Construct workflow def (orchestration calling run.Step(step, input)).
	•	Register steps + workflows during service initialization (using the port).
	•	Store the returned RegisteredWorkflow handle(s).
	•	Business-facing methods call handle.Run(...).GetResult(...) like normal.

Where logic lives
	•	Step bodies: in step defs (domain logic, repo calls, external effects).
	•	Orchestration: in workflow def (the ordering/branching of steps).
	•	No DBOS/go-workflows imports anywhere in these packages.

⸻

Backend mapping rules

DBOS implementation (infra adapter)
	•	RegisterWorkflow(wf):
	•	wrap app workflow into a DBOS workflow function:
	•	DBOS calls func(dctx DBOSContext, input I) (O, error)
	•	adapter creates a Run that captures dctx
	•	calls wf.Run(run, input)
	•	call dbos.RegisterWorkflow(rootCtx, wrapperFn)
	•	return a handle that runs via dbos.RunWorkflow(rootCtx, wrapperFn, input) and wraps DBOS’s workflow handle.
	•	Run.Step(step, input):
	•	implement as dbos.RunAsStep(dctx, func(ctx context.Context) (O, error) { return step.Run(ctx, input) })
	•	DBOS durability comes from checkpointing step outputs and returning recorded results during recovery.
	•	RegisterStep(step):
	•	often a no-op for DBOS (kept for portability).

go-workflows implementation (infra adapter)
	•	RegisterWorkflow(wf):
	•	register a go-workflows workflow function that:
	•	creates a Run backed by go-workflows workflow context
	•	calls wf.Run(run, input)
	•	RegisterStep(step):
	•	register each step as an activity
	•	Run.Step(step, input):
	•	call workflow.ExecuteActivity(step.Run, input) and wait for result
	•	Handle.GetResult:
	•	map to the engine’s workflow instance result retrieval

Important: workflow code must remain deterministic across engines. Orchestration should not call time/random/network directly; put those behind steps.

⸻

Determinism and idempotency guidance (LLM should enforce)
	•	Workflow body (Workflow.Run) should:
	•	only orchestrate steps, branch on returned step values, and manipulate pure data
	•	avoid direct side effects
	•	Step bodies (Step.Run) may do side effects but must be safe under retries:
	•	add idempotency keys or “already done” guards for external calls (payments, email, publish)
	•	database steps should be bounded transactions

⸻

Quality checks (“litmus tests”)

An abstraction is “portable” if:
	•	Steps are explicit defs with stable identity + typed inputs (no closures).
	•	Workflow orchestration is framework-free and only uses run.Step.
	•	You can implement Run.Step using:
	•	DBOS RunAsStep + step.Run(ctx, input)
	•	go-workflows ExecuteActivity(step.Run, input)

It’s “clean” if:
	•	application packages compile with zero DBOS/go-workflows imports
	•	infra is the only place that imports those libraries

⸻

What to generate when asked for code

When asked to implement a workflow:
	•	define Step types for each durable boundary
	•	define a Workflow type orchestrating them with run.Step(step, input)
	•	in the service constructor: register steps then register workflow and store handle
	•	in service method: invoke stored handle and return result

When asked to implement a new backend:
	•	implement Registry, RegisteredWorkflow, Handle, and Run
	•	map Run.Step to the engine’s durable primitive:
	•	DBOS: RunAsStep
	•	go-workflows: ExecuteActivity