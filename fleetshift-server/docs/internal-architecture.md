# Internal Architecture

The code follows a "hexagonal" / layered architecture style, with DDD tactical patterns.

Layers:

1. **Main (Bootstrap / Application):** Process entry and exit point. Config parsing and initial object graph. It is the only layer that depends on Infrastructure. Only logic is to create objects from user input, start them, and stop them.  
2. **Presentation / Transport:**  Protocol specifics, request validation, credential validation, mapping to value objects. Depends on application/domain.  
3. **Application Services:** Transaction boundary, (meta) authorization, observability, published available commands and queries. Used by Main (CLI commands) and Presentation (commands available over the wire).  
4. **Domain:** The domain model. 95% of business logic is here. Interfaces for "external" dependencies are here.  
5. **Infrastructure:** Implementations of interfaces used across layers. Not depended on directly by anything except Main.

## Main (Bootstrap / Application)

This is the layer that contains the process entrypoint. It deals with configuration and bootstrapping the application. A dependency graph is instantiated here.

* main.go  
* CLI commands (just enough to invoke Application Services)  
* Config schemas for deserialization from CLI options, config files, env vars, etc. (this is stable API)  
* Object graph instantiation and lifecycle (e.g. hot reload, if supported)

CLI commands should not have substantial logic. This is reserved for Application Services. Tests or one-offs can still involve services or model updates and often benefit from doing so as it makes them testable.

### Tests

Tests here are focused on the meaning and validation of config options, and maybe a few tests that run substantial commands with in-memory (hermetic) dependencies. Tests are few because the point of this module is to couple to the outside world, and this makes it difficult to test. We push that complexity here in order to make everything else more testable.

## Presentation / Transport

This layer is responsible for supporting inbound communication over a network. For example, with gRPC, this is where the implementations of the generated gRPC servers go. Middleware is used for cross cutting and protocol specific concerns like authentication (as credential presentation is necessarily protocol specific) or pagination.

Server implementations here are *small*. They only care about mapping between *Application Services* and the *protocol*. There is no business logic here. It depends on Application Services and the Domain, as Application Services' interfaces speak in terms of the Domain Model.

### Tests

Tests here focus on protocol specifics, such as handling of errors and serialization edge cases. It should attempt to stand up the presentation layer as production-like as possible, with the same middleware. They instantiate in memory dependencies where I/O may be normally involved. They may need to stand up a real network service, to test the presentation. However, many presentation layers can use in-memory transports to avoid this (e.g. [bufconn](https://pkg.go.dev/google.golang.org/grpc/test/bufconn) with gRPC).

### But what about request validation or normalization? Isn't that business logic?

It's presentation logic. Let's look at why and how to avoid confusing this with what *is* business logic.

Whenever you invoke another method in an application, that method has certain preconditions that must be true. If those preconditions are not true, it returns an error. If you can prevent this error ahead of time, you should. You'll have more context about what is wrong. You won't leak an underlying API's details.

Invoking the Application Services from Presentation is an example of this. The Application Services' commands and queries will have certain preconditions. The presentation layer should ensure these conditions are met *before* invoking the Application Service. Errors such as these from the application service may be considered server errors. Errors caught in Presentation can be clearly classified as bad input. It does not mean the caller is responsible for preemptively identifying all error scenarios. Of course, that is the job of the logic of the method. Only *preconditions* that are clearly documented as part of the method's contract should be checked.

This is not to be confused with *domain model* validation or normalization. This is not a *substitute* for validation in the domain. It is in addition to it. Only as a little as necessary is done in presentation to make for useful errors according to the protocol.

## Application Services

All of the commands and queries which can be invoked directly from outside the process are defined here. This includes the public API. It also includes commands that may be only available through CLI tools and admin interfaces and the like. The application services API is defined here, leveraging types from the model. It can define its own interfaces IF they are only intended to be used by application services. Model services MUST NOT depend on the application service layer. Implementations of these interfaces usually live in infrastructure, just like with the model. The only exception may be interface implementations which have no external dependency.

This layer makes the end to end use cases of an application testable, without any coupling to the I/O concerns of external process communication (e.g. CLI input or requests over the network) that exist with presentation layers. It makes the use cases reusable from different protocols.

### Tests

Tests here focus on the use cases of the application. They may not hit every branch in the domain model, but should hit nearly every branch possible from the external API. Use fakes for I/O (e.g. repositories).

## Domain

This is the primary home for business logic that differentiates the application. Domain is usually *flat and wide*. It is a large package, with little to no hierarchical structure. Nesting quickly gets arbitrary, requires duplication, or creates import cycles. A domain model reuses its rich types throughout, almost never relying on primitives except for internal implementation detail. Interfaces are defined here that the model depends on internally. Implementations are usually defined in infrastructure. A domain layer only ever depends on itself. It rarely depends on third party code. It never performs I/O.

Sometimes you can define interfaces' implementations in domain, while obeying these dependency rules. Some implementations are simple enough (a very basic fake for a simple interface) that it may be convenient to do so. Others may be core aspects of the domain model itself (e.g. strategy implementations that are part of the business rules). In those cases, the implementations MUST be defined in domain, with dependency rules enforced (I/O abstracted).

### Domain Model vs Data Model

A Domain Model is commonly confused with a database's *data model*. In some communities (e.g. Java), it is common that they are even defined together. They are not the same thing.

A domain model defines the nouns and verbs of the business. We express business rules as directly and simply as possible in imperative code. It matches how the entire team, from coders to UX, speak about the problem domain. It necessarily contains data (state), but it is not the same thing as the data model used by a database to persist this state. It is the responsibility of the *repository implementation* to define how it translates the state of the domain model into database state, and back. However, domain models often participate in *enabling this* by exposing a serialized form.

This can be explicit or declarative. In an explicit model, the domain model has explicit serialize/deserialize functions that expose the raw state of the model, and hydrate from raw state. In a declarative model, the domain model is annotated (in some language dependent way) with how it can be serialized. Declarative metadata can "get away" with describing repository implementation details in the domain model, because it is just that: declarative metadata. Good metadata frameworks are flexible enough such that they don't otherwise impose themselves on the domain model. An ideal domain model makes no concessions to frameworks. Its only concern is modeling the business problem accurately and correctly.

We use an explicit approach. The domain model can be serialized and deserialized to/from generic JSON-like structures. From here, the repository layer can transform this however it needs for its persistence.

### Tests

Tests here are true "unit tests." They can usually isolate a single method or struct. They tend to hit all branches because the units are so small. Tests are small, instant, and numerous. 

## Infrastructure

Infrastructure is where implementations of interfaces defined elsewhere go. These are not the essence of the application, but adapters to make the essential abstractions work in certain environments or dependencies. Implementations that require I/O in particular almost always go here. There is little business logic here. Some business rules are expressed insofar as they are requirements of their interfaces. As such, it is not the responsibility of infrastructure to define business logic, but it may have to adhere to it. For example, a repository has to understand the domain model enough in order to enforce constraints and follow query parameters.

### Tests

Tests in infrastructure are usually ["medium" or "large"](https://testing.googleblog.com/2010/12/test-sizes.html) in the sense that they typically, by design, require I/O or an external system. This is one of the reasons we isolate this code.

#### Contract Tests

Tests in infrastructure often benefit from being designed as "contract tests" which are reusable for other implementations. "Contract tests" are defined *where the interface is defined, not the implementation*. They speak only in terms of a factory (to get some implementation) and clean up (cleaning up external resources). Then, they exercise that interface to demonstrate expectations. The actual test runner exists in infrastructure for a specific implementation. It invokes the contract tests, providing only the necessary factory and clean up. [Example](https://github.com/alechenninger/falcon/blob/ae638df2a195b903a76e414db00d3aa32078a09a/internal/domain/storetest.go). This makes it easy to:

* document the expectations of an interface in terms of the business language and domain model  
* test different implementations adhere to it  
* …which is especially important for when an implementation necessitates I/O, and therefore you want a correct fake in-memory implementation to also pass tests such that it is a confident substitute for the real thing.