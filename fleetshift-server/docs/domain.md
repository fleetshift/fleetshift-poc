# Domain

The domain package (see [internal-architecture.md](./internal-architecture.md)) is where the bulk of the business logic lives.

Logic should be pushed down as much as possible, without creating undue coupling. Domain objects are the lowest level with which to push down logic.

Logic about an object, like its state transitions, should be pushed to that struct. A struct's fields should almost never be manipulated directly, except by that struct's methods, if that struct lives in the domain model. Encode invariants in these methods to help reduce bugs and security issues in the code.
