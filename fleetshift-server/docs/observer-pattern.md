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
    // Result is called with the operation result.
    Result(...)
    
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

func (NoOp{Op}Probe) Result(...) {}
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
    
    if err != nil {
        probe.Error(err)
        return err
    }
    
    probe.Result(...)
    return nil
}
```

## Key Principles

- Always use `defer probe.End()` for timing accuracy
- Probes either emit signals throughout method calls, or may collect state via methods and only emit upon `End()`. It depends on signal best practices and what minimizes overhead. Logs usually emit at the end, unless the probe runs long.
- Domain interfaces live in `domain/` or `application/`; implementations in `observability/`
- Include `request_id` from context in logs
- Check log level before constructing log messages

## Method Parameters

- **Cheap to obtain**: Parameters should already be available at the call site. Avoid requiring expensive computation, allocations, or I/O just to call an observer method.
- **Informative**: Parameters should provide enough context for useful logs, metrics, or traces (e.g., IDs, counts, interesting details, error details). Optimize for minimium runtime cost while maximizing information available to probes.
- **Domain-oriented**: Use domain types rather than primitives where practical. Speak in the language of the model.
