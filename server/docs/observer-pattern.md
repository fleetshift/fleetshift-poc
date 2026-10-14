# Observer Pattern

Use when adding observability to domain or application components. Inspired by [Domain-Oriented Observability](https://martinfowler.com/articles/domain-oriented-observability.html).

## Structure

1. **Observer interface** - Entry point, returns Probes for operations
2. **Probe interface** - Tracks a single operation lifecycle  
3. **NoOp implementations** - For forward compatibility and testing

## Naming

- Observer: `{Component}Observer` (e.g., `WriteObserver`)
- Probe: `{Operation}Probe` (e.g., `WriteProbe`)
- NoOp: `NoOp{Component}Observer`, `NoOp{Operation}Probe`
- Variable at call site: `probe` (e.g., `ctx, probe := ...`). Each probe lives in its own scope, so there should be no need to disambiguate.

## Interface Pattern

```go
// {Component}Observer is called at key points during {Component} operations.
// Implementations should embed NoOp{Component}Observer for forward compatibility
// with new methods added to this interface.
type {Component}Observer interface {
    // {Op}Started is called when {Op} begins.
    // Returns a potentially modified context and a probe to track the operation.
    {Op}Started(ctx context.Context, ...) (context.Context, {Op}Probe)
}

// {Op}Probe tracks a single {Op} invocation.
// Implementations should embed NoOp{Op}Probe for forward compatibility.
type {Op}Probe interface {
    // Operation-specific methods for recording outcomes, state
    // changes, or branch decisions (e.g. Stale, Persisted,
    // Dispatched). Shape these to the operation being observed.
    
    // Error is called when an error occurs.
    Error(err error)
    
    // End signals the operation is complete (for timing). Called via defer.
    End()
}
```

## NoOp Implementation

Always provide NoOp implementations in the same package as the interface:

```go
type NoOp{Component}Observer struct{}

func (NoOp{Component}Observer) {Op}Started(ctx context.Context, ...) (context.Context, {Op}Probe) {
    return ctx, NoOp{Op}Probe{}
}

type NoOp{Op}Probe struct{}

// ... no-op stubs for operation-specific methods ...
func (NoOp{Op}Probe) Error(error) {}
func (NoOp{Op}Probe) End() {}
```

## Example: Logging Implementation (observability package)

```go
type {Component}Observer struct {
    domain.NoOp{Component}Observer  // Embed for forward compatibility
    logger *slog.Logger
}

func New{Component}Observer(logger *slog.Logger) *{Component}Observer {
    return &{Component}Observer{logger: logger.With("component", "{component}")}
}

func (o *{Component}Observer) {Op}Started(ctx context.Context, ...) (context.Context, domain.{Op}Probe) {
    return ctx, &{op}Probe{
        logger:    o.logger,
        ctx:       ctx,
        startTime: time.Now(),
        // capture input params...
    }
}

type {op}Probe struct {
    domain.NoOp{Op}Probe  // Embed for forward compatibility
    logger    *slog.Logger
    ctx       context.Context
    startTime time.Time
    // state fields...
}

func (p *{op}Probe) End() {
    if p.err != nil {
        p.logger.LogAttrs(p.ctx, slog.LevelError, "{op} failed", ...)
        return
    }
    if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
        return
    }
    p.logger.LogAttrs(p.ctx, slog.LevelInfo, "{op} completed",
        slog.Duration("duration", time.Since(p.startTime)), ...)
}
```

## Usage in Domain Code

```go
func (s *Service) DoOperation(ctx context.Context, ...) error {
    ctx, probe := s.observer.DoOperationStarted(ctx, ...)
    defer probe.End()
    
    // ... operation logic ...
    // Call operation-specific probe methods at branch points
    // (e.g. probe.Stale(), probe.Persisted(), etc.)
    
    if err != nil {
        probe.Error(err)
        return err
    }
    return nil
}
```

## Key Principles

- Always use `defer probe.End()` for timing accuracy
- **Never replace the caller's context.** `Started` methods must pass the incoming `ctx` through (or enrich it, e.g. by injecting a trace span). Returning `context.Background()` or any unrelated context severs request-scoped values (trace IDs, deadlines, cancellation) from downstream code. NoOp implementations return the incoming `ctx` unchanged.
- Constructors should default the observer field to a NoOp implementation (e.g. `observer: NoOp{Component}Observer{}`). This eliminates nil checks at every call site.
- Probes either emit signals throughout method calls, or may collect state via methods and only emit upon `End()`. It depends on signal best practices and what minimizes overhead. Logs usually emit at the end, unless the probe runs long.
- Domain interfaces live in `domain/` or `application/`; implementations in `observability/`
- Include `request_id` from context in logs
- Check log level before constructing log messages

## Multi-Layer Probes (Durable Workflows)

When observing durable workflows with activities, probes **cannot cross the workflow/activity serialization boundary** because they hold non-serializable state (contexts, loggers, timing). This produces a two-layer architecture:

- **Workflow-level probes** are created in the deterministic workflow body and observe control flow (signal arrivals, retries, state transitions, phase progression).
- **Activity-level probes** are created fresh inside each activity closure from the same observer. They observe I/O-bearing work (transaction branches, dispatch decisions, data mutations).

A single observer can return both layers. A workflow-level probe may also spawn **hierarchical sub-probes** for long-running phases (e.g. a dispatch-and-await cycle). Sub-probes follow the same `Started → ... → End()` pattern but may omit the context return when the parent already holds the relevant context.

See `FulfillmentObserver` in `domain/fulfillment_observer.go` for the canonical example.

## Method Parameters

- **Cheap to obtain**: Parameters should already be available at the call site. Avoid requiring expensive computation, allocations, or I/O just to call an observer method.
- **Informative**: Parameters should provide enough context for useful logs, metrics, or traces (e.g., IDs, counts, interesting details, error details). Optimize for minimium runtime cost while maximizing information available to probes.
- **Domain-oriented**: Use domain types rather than primitives where practical. Speak in the language of the model.
- **Started vs. probe methods**: If data is unconditionally available at the call site and always passed to the probe, put it in the `Started` signature. Probe methods are for signals that occur conditionally during the operation (branch decisions, outcomes, errors). A probe method that is always called immediately after `Started` is a sign the data belongs in `Started` instead.
