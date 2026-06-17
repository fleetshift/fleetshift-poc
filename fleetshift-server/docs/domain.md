# Domain

The domain package (see [internal-architecture.md](./internal-architecture.md)) is where the bulk of the business logic lives.

Logic should be pushed down as much as possible, without creating undue coupling. Domain objects are the lowest level with which to push down logic.

Logic about an object, like its state transitions, should be pushed to that struct. A struct's fields should almost never be manipulated directly, except by that struct's methods, if that struct lives in the domain model. Encode invariants in these methods to help reduce bugs and security issues in the code.

## Aggregates, entities, and value objects

There are three kinds of domain objects, each with different ownership and
identity semantics.

**Value objects** are the simplest and most preferred. A value is defined
entirely by its data — two values with the same fields are equal. Values have
no lifecycle and are freely copied. They enforce their own invariants through
constructors, even when they are just type aliases over primitives (e.g.
`NewServiceName`, `NewCollectionID`). Prefer values wherever possible.

**Entities** have identity and a lifecycle but do not form a consistency
boundary on their own. They are always owned by an aggregate and accessed
through it. For example, `ResourceRepresentation` is an entity within the
`PlatformResource` aggregate — it has identity (service + collection + name)
and mutable state, but is never loaded or persisted independently.

**Aggregates** are consistency boundaries. An aggregate owns its entities and
values, and all mutations go through the aggregate root's methods. External
code never reaches into an aggregate to mutate a child entity directly. The
aggregate enforces invariants that span multiple fields or child entities (e.g.
"a representation's collection must match the aggregate's collection"). The
repository loads and saves entire aggregates, not individual child entities.

### Where logic belongs

- **Value object constructors** enforce the invariant of that single value
  (format, non-emptiness, allowed characters). Callers must use the
  constructor; the type system prevents constructing invalid values.
- **Aggregate methods** enforce invariants that require looking across multiple
  fields or entities within the aggregate. For example, role exclusivity rules,
  cross-entity uniqueness within the aggregate, or collection matching.
- **Repositories** enforce storage-level constraints: cross-aggregate
  uniqueness (e.g. alias ownership across different platform resources),
  referential integrity, and transactional consistency. Repository logic should
  be storage-oriented — translating aggregate state into SQL, handling
  conflicts, reconciling — not business logic.
- **Application services** coordinate work that spans an aggregate and
  something else, or orchestrate several composable aggregate methods into a
  higher-level use case (e.g. claim-or-get with alias attachment). Services
  should not duplicate logic that the aggregate already enforces.

If logic can live in a value constructor, it should. If it requires aggregate
context, it belongs on the aggregate. Only when it truly spans aggregates or
requires external coordination does it belong in a service.

### Self-enforcing invariants at each layer

Each layer trusts that layers below it have already enforced their invariants.
A value object, once constructed, is known to be valid. An aggregate method
does not re-validate value objects passed to it — it only checks cross-field
invariants that the values cannot enforce alone. An application service does
not re-validate what the aggregate already checks — it only enforces cross-
aggregate or coordination-level invariants. The transport layer constructs
value objects and commands from raw input, and those constructors are the first
line of validation.

## Snapshots and persistence

Every domain aggregate that participates in a repository has a corresponding
**snapshot** type — an all-exported, anemic DTO used as the explicit
serialization boundary between the domain and persistence layers. The pattern
eliminates two problems: (1) domain types no longer need exported fields,
preserving encapsulation; (2) hydration logic lives in one factory per type
rather than scattered across repository scan functions.

### Anatomy of the pattern

Each aggregate `Foo` has:

| Piece | Location | Purpose |
|---|---|---|
| `FooSnapshot` | `snapshot.go` | All-exported struct with primitives, value objects, or nested DTOs. No behavior. |
| `(f *Foo) Snapshot()` | `snapshot.go` | Captures current state (including pending write buffers) into a snapshot. |
| `FooFromSnapshot(s)` | `snapshot.go` | Factory: hydrates a domain object from a snapshot. Pending buffers start empty; internal baselines (e.g. `loadedGeneration`) are derived from persisted state. |
| `MarshalJSON / UnmarshalJSON` | `snapshot.go` | Delegates to `Snapshot()` / `FromSnapshot()` so domain objects survive `encoding/json` round-trips (required by the memworkflow fidelity pass). |
| Accessor methods | aggregate source file | Read-only access to private fields for external callers. |

### Rules

- **Snapshot fields are pure data.** Primitives, type aliases, value objects,
  or slices/maps thereof. Never embed a domain object inside a snapshot.
- **Pending buffers are write-path only.** `Snapshot()` captures pending
  strategy records / intents non-mutatively. `FromSnapshot()` always
  initializes them as empty — the object starts in "freshly loaded" state.
  Repositories use the explicit drain methods (`DrainPendingStrategyRecords`,
  `DrainPendingIntents`) to extract and clear pending buffers for flushing
  to storage, ensuring each record is written exactly once.
- **Repositories use snapshots for both reads and writes.** Write paths call
  `obj.Snapshot()` and read individual fields. Read paths scan into a snapshot
  struct and call `FooFromSnapshot(s)` to produce the domain object.
- **External callers use constructors and accessors.** Code outside the `domain`
  package must never construct aggregates via struct literals. Use `NewFoo(...)`
  for creation and `FooFromSnapshot(s)` for reconstitution; use `obj.Field()`
  for reads.
- **JSON marshaling is snapshot-based.** All aggregates implement
  `MarshalJSON` / `UnmarshalJSON` via snapshots. This is critical for the
  memworkflow engine's JSON fidelity pass and for any future durable engine
  that serializes activity inputs/outputs. Use value receivers for
  `MarshalJSON` so non-addressable struct values (e.g. fields inside a view
  passed by value) can still be marshaled.

## Field access and getters

Within a struct's own methods, direct private field access is natural and fine.
Outside of that — even within the same package — prefer the public getter
methods. This keeps the call sites honest about encapsulation and makes it
straightforward to add lazy computation, caching, or invariant checks to a
getter later without auditing every caller. The rule applies to production code
and tests equally.

Intentional exceptions:

- **Snapshot / FromSnapshot / JSON marshaling** in `snapshot.go` — these are
  the explicit serialization boundary and must touch raw fields.
- **Constructors** (`NewFoo(...)`) — struct-literal initialization is the
  whole point.
- **Etag computation** (`etag.go`) — these functions hash raw aggregate state
  to produce opaque tokens and are tightly coupled to the serialization shape
  by design.

## Constructors vs snapshots

Domain aggregates have **two** construction paths:

| Path | Function | When |
|---|---|---|
| `NewFoo(...)` | Creates a brand-new object that has never been persisted. | Creation workflows, factory methods. |
| `FooFromSnapshot(s)` | Reconstitutes an existing object from persisted state. | Repository read paths, JSON deserialization. |

The `New*` constructors enforce creation-time invariants (e.g. initial state,
required fields) at compile time via their parameter list. They leave internal
persistence baselines (like `loadedGeneration`) at zero since the object has
never been loaded from storage. After construction, callers use domain methods
(e.g. `AdvanceManifestStrategy`) to complete initialization.

`FromSnapshot` is reserved exclusively for the persistence boundary — it
assumes the data has already been validated and sets internal baselines from
the stored values.

### Zero-value returns

Within the domain package, error-path returns that need a typed zero value
should use the Go zero literal directly (`AuthMethod{}`, `SignerEnrollment{}`)
rather than `FooFromSnapshot(FooSnapshot{})`. Outside the domain package (where
private fields prevent struct-literal construction), callers should declare a
named return or use `var zero Foo`.

## Adding a new aggregate

1. Define the private-field struct in its source file with accessor methods.
2. Add a `NewFoo(...)` constructor for the creation path.
3. Add `FooSnapshot` to `snapshot.go`.
4. Add `Snapshot()`, `FooFromSnapshot()`, `MarshalJSON`, and `UnmarshalJSON`
   to `snapshot.go`.
5. Add a round-trip test in `snapshot_test.go`.
6. Have repositories scan into the snapshot and call `FooFromSnapshot`.

## Test helpers

When test helpers need to seed a domain object, tests should avoid trying to
"force" the desired state. Prefer to simply set up the desired state exactly
as the application is designed to set it up. If the state is unreachable, then
it must not matter to test by definition. If the state is reachable, then use
the conventional contracts of the objects to reach it. Example: if you want to
test an object at a later generation N, perform N - 1 read, modify, write 
cycles to get to that generation.
