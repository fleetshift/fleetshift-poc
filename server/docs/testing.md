# Testing

Tests are done deterministically and [hermetically](https://testing.googleblog.com/2018/11/testing-on-toilet-exercise-service-call.html).

## Test-driven development

Prefer to write tests first, then passing code. This is especially true when there is a bug. If there is a bug, ALWAYS start by writing a failing test to capture the correct understanding of the bug and ensure the test is valid. A test we've never seen fail should not be trusted as actually catching the failure and thus the fix.

## Scenarios

Tests should not only exercise "happy paths." Edge cases, like odd sequences of requests (repeat failures, double requests, etc), should be tested.

## No I/O

By default, there is NO external I/O in tests. This often includes syscalls (e.g. time, randomization). This means you often need to design main code to support testability:

* Instead of using time directly, inject a Clock abstraction, OR use [synctest](https://go.dev/blog/testing-time)  
* Instead of databases or queues, use in memory fakes  
* Instead of using the filesystem directly, inject a filesystem abstraction (either [io/fs](https://pkg.go.dev/io/fs) or something more full featured like [afero](https://github.com/spf13/afero))  
* Instead of using randomness directly, inject an abstract source of randomness and use a deterministic version for tests

Never use time.Sleep in a test. Use a clock abstraction to advance the time, or [deterministic concurrency](#deterministic-concurrency).

The only exception is when the code under test itself is necessarily coupled to external I/O. If you have a PostgresRepository, you obviously have to test it by connecting to a postgres instance. But if you aren't specifically testing the implementation of something dependent on I/O, avoiding it will improve your tests and your designs.

## Observability

Observability is often untested or awkward to test. Take advantage of the [Domain Oriented Observability](https://martinfowler.com/articles/domain-oriented-observability.html) pattern. We don't need excessive coverage of observability concerns, as these are often tested automatically by virtue of alert rules on metrics. Testing observability is therefore a judgement call on the importance, complexity, and how likely and how quickly a regression is to be caught in production under normal operation. When it is warranted though, this pattern makes it much simpler to do so.

For real world examples in a Go codebase, see [this](https://github.com/project-kessel/parsec/blob/main/internal/service/observability.go) and [this](https://github.com/alechenninger/falcon/blob/main/internal/domain/observer.go).

## Deterministic concurrency

Coordinating threads / goroutines is sometimes necessary in tests. To do this deterministically and cleanly, take advantage of the [Domain Oriented Observability](https://martinfowler.com/articles/domain-oriented-observability.html) pattern. The main code is coupled on to an interface with certain probe points. Then, an implementation of this injected at test time uses these probes to block, or signal waiting code.

For an example of how to do this, see [this](https://github.com/alechenninger/falcon/blob/ae638df2a195b903a76e414db00d3aa32078a09a/internal/domain/observer.go#L252).

## Reusable test doubles – No Mocks (and RARELY stubs!)

No "method verifying" mocks, ever.

Prefer simply using a real instance. If an object is not coupled to external I/O, there is no reason not to reuse it. It is the least work and the best coverage.

If it is, prefer using a Fake. In memory fakes are a useful feature of an application ("Kessel in a box"), so the investment pays for itself quickly. When implementing fakes (or any second implementation of an interface), first define a set of "contract tests" at the interface layer.

When there is a fake, reuse it liberally, even if the full functionality is not needed. It is fine to add methods to existing fakes as well. The idea is that a fake is a complete and holistic feature of the application, built for  aiding testing. When a new test double is needed that is not a complete fake, also design it to be reusable, and prefer to expand on it, rather than adding many one-off stubs.

Stubs or dummies can be used judiciously when the interaction is completely trivial, or there is little reuse to be had. This is rare. There is usually no point if there is a fake and the in memory version is just as fast.

## Hermetic

When external dependencies are needed, leverage testcontainers to download and run them locally. This should only be for when this is essential. For example, we can't test a PostgresStore without a Postgres. Writing a "fake" postgres is absurd 🙂. But, if you need to test business logic that involves a repository, using a real postgres is overkill. Just use the in memory fake (e.g. a custom in memory implementation, or sqlite with an in memory database, etc.).

## Integration tests (Docker-heavy)

Tests that spin up heavyweight Docker infrastructure (Kind clusters, multi-container test environments, etc.) are gated behind a build tag:

```go
//go:build integration
```

This means these files are **excluded from compilation** by default. They only compile and run when you explicitly pass `-tags integration`.

### Convention

- File naming: `*_docker_test.go` signals that the file requires the integration tag.
- The build tag is the first line of the file, followed by a blank line before the package clause.
- Do not combine `testing.Short()` guards with the build tag; the tag is the gate.

### Running locally

The default test suite excludes integration tests automatically:

    go test ./...

To run integration tests explicitly (requires Docker or Podman):

    go test -tags integration ./internal/addon/kind/

### CI behavior

CI runs the default suite (`go test ./...`) first. A separate step runs the integration tests with `-tags integration` for the `fleetshift-server` module. This avoids Docker resource contention between the two sets.

### Why build tags instead of `-short`?

Build tags are compiler-enforced. If a developer writes a new Docker-heavy test and forgets the tag, it compiles into the default suite and fails loudly from resource contention -- rather than being silently skipped forever (which is the `-short` failure mode).