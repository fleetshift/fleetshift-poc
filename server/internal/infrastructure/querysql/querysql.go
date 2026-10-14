// Package querysql compiles a CEL filter, evaluated against
// QueryResources' result envelope, into a parameterized SQL
// predicate.
//
// # Canonical ProtoJSON-shaped input
//
// QueryResources filters are a strict subset of CEL over the
// canonical ProtoJSON-shaped query result:
//
//   - Message fields are exposed by their canonical JSON name only
//     (normally lowerCamelCase, or an explicit json_name).
//   - Map and google.protobuf.Struct keys are exact, case-sensitive
//     data keys.
//   - Filter input is never case-normalized and has no
//     proto-name/JSON-name aliases.
//   - Select and string-index syntax with the same raw key are
//     equivalent (standard CEL map semantics).
//
// Authors should copy field names from the JSON response. Use
// brackets when an exact map key is not a legal CEL selector.
//
// # Why not github.com/spandigital/cel2sql/v3
//
// github.com/spandigital/cel2sql/v3 (v3.8.8 at evaluation time) is the
// most prominent Go CEL-to-SQL library and was evaluated hands-on
// rather than assumed. It does not fit. That library does target
// Postgres by default (it is genuinely multi-dialect -- Postgres,
// MySQL, SQLite, DuckDB, BigQuery, Spark -- with real JSON/JSONB and
// parameterized-query support), so the objection isn't "wrong
// database". Two concrete incompatibilities with this package's
// required field set surfaced from compiling representative filters
// through it directly:
//
//  1. Map-keyed JSONB access nested under a dynamically-typed parent
//     -- exactly this package's resource.labels["team"],
//     resource.localLabels[...], and
//     resource.conditions["Ready"].status shapes -- does
//     not compile to a keyed lookup. `resource.labels["team"] ==
//     "platform"` compiled (across every schema declaration style
//     tried: WithJSONVariables, an opaque WithSchemas entry, and a
//     structured nested WithSchemas entry) to
//     `resource->>'labels'[1] = 'platform'`: the string key is
//     discarded and replaced with a literal array index, which is
//     wrong SQL, not merely suboptimal SQL. The chained
//     conditions["Ready"].status shape produced invalid SQL
//     (`resource->'inventory'->>'conditions'[1].status`) under every
//     tested declaration. Labels are a common field across every
//     resource kind in this data model, so this isn't a corner case.
//  2. Its schema model (schema.Schema/FieldSchema) describes one
//     fixed, closed-world shape per compiled expression -- there is
//     no per-row discriminator concept. This package's
//     resource.spec.*/resource.observation.* fields are
//     read from a JSON column whose *shape differs by
//     resourceType*, resolved only once resourceType == "..." is
//     known (see hasResourceTypeGuard), across a single query
//     spanning every extension resource type. A library built
//     around one static schema per Convert call has no hook for that.
//
// Given both, this package instead implements the documented
// supported CEL subset directly over cel-go's parser/checker, behind
// a [CELSQLCompiler] interface named around the role a cel2sql-style
// dependency would play, so repository code does not need to change
// if a future library fixes these gaps.
//
// # Package split
//
// This package owns CEL AST lowering: boolean/logical structure,
// comparison, "in" (value-list and container membership
// orchestration), startsWith, has()/presence, literal binding
// (including [ParseCELTimestamp] for timestamp() string literals),
// and resourceType guard detection (compiler.go). It also owns
// dialect-neutral ProtoJSON path validation and container
// classification ([ValidateDescriptorPath], ResolveContext's
// ValidateSpecPath / ValidateObservationPath /
// ClassifyResourceContainer), wired through an optional
// [Compiler.Schemas]. Column names, JSON extraction SQL,
// label/condition map keys, dialect boolean/collation spelling,
// projection-faithful presence predicates, list/unknown JSON
// membership SQL, and timestamp/JSON SQL rendering remain the concern
// of whatever [FieldResolver] the caller supplies (see
// field_resolver.go and the postgres/sqlite packages' query_filter.go
// + query_expr_*.go). This split exists because querysql's supported
// CEL subset and schema-shaped classification are QueryResources-wide
// contracts — any storage backend would parse and classify filters
// the same way — while the row shape a field path resolves to is
// backend-specific.
//
// Parameter placeholder style is likewise a dialect concern, owned
// by the caller's [ParamBinder] (see param_binder.go). [Compiler.Params]
// is required; there is no default binder.
//
// Supported filter shape: see compiler.go for the supported operators
// (&&, ||, !, ==, !=, <, <=, >, >=, in, startsWith, has, timestamp)
// and field-path syntax (identifiers, dotted selects, and
// string-keyed index expressions). Timestamp response fields remain
// ProtoJSON strings (direct comparison uses the canonical spelling);
// chronological / instant comparisons use timestamp() on both sides
// and may wrap any string-valued path. Presence uses CEL has(select)
// (not has(index)); container membership uses `"key" in <field path>`
// alongside the existing `field in [literals]` value-list form.
// Anything else -- unsupported operators, arithmetic, regex,
// endsWith/contains/matches, and exists/all/map/filter macros --
// fails closed with [domain.ErrInvalidArgument], as does any field
// path a configured [FieldResolver] doesn't recognize.
package querysql

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/cel-go/cel"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// CELSQLCompiler compiles a CEL filter into a parameterized SQL
// predicate. Exists as an interface -- rather than a bare function --
// so repository code depends on this role rather than a concrete
// cel-go wiring.
type CELSQLCompiler interface {
	CompileFilter(ctx context.Context, in CompileFilterInput) (SQLPredicate, error)
}

// CompileFilterInput is [CELSQLCompiler.CompileFilter]'s input.
// Filter is the raw CEL expression from
// [domain.QueryResourcesRequest.Filter]. Compile treats an empty
// Filter as "match everything"; callers may also short-circuit empty
// filters themselves to skip compilation entirely.
type CompileFilterInput struct {
	Filter string
}

// SQLPredicate is a compiled filter: a boolean SQL expression plus
// the ordered bind parameter values its placeholders reference.
// SQL never contains user-supplied *values* -- every literal in the
// filter is bound through builder.bind, and every field path is
// either a static column name or run through the configured
// [FieldResolver], which must do the same for any value it needs to
// inline (see [ResolveContext.Bind]'s doc). Placeholder spelling is
// controlled by the compiler's [ParamBinder].
type SQLPredicate struct {
	SQL  string
	Args []any
}

// Compiler is the only [CELSQLCompiler] implementation. It is safe
// for concurrent use as long as Fields, Params, and Schemas are (or
// are nil/immutable). CompileFilter shares a package-level *cel.Env
// (see filterCELEnv) whose declarations never change, so concurrent
// Compile calls are safe once that env has been initialized.
type Compiler struct {
	// Fields resolves the field paths a filter references (envelope
	// columns, resource.*, ...) to SQL
	// expressions. A nil Fields is only valid for filters that
	// reference no fields at all (e.g. the empty filter, or a filter
	// built entirely from macros/literals -- both already rejected
	// for other reasons); any real filter compiled against a nil
	// Fields fails with a descriptive error rather than a nil-pointer
	// panic.
	Fields FieldResolver

	// Params formats bind-parameter placeholders in the generated
	// SQL. Required: a nil Params fails CompileFilter with a
	// descriptive error. Each storage backend supplies its own
	// ParamBinder (Postgres $N, SQLite ?N).
	Params ParamBinder

	// Schemas, when set, lets ResolveContext validate
	// resource.spec.*/resource.observation.* paths and classify
	// containers against real protobuf descriptors when the filter's
	// top-level resourceType guard resolves to a registered type.
	// Nil is a valid permissive default (structural acceptance only).
	// Lookups are lazy and reused for the duration of one
	// CompileFilter call; they are not cached across compilations.
	Schemas domain.QuerySchemaProvider
}

var _ CELSQLCompiler = Compiler{}

// filterCELEnv is the shared CEL environment for every CompileFilter
// call. Variable declarations are fixed (see newCELEnv), so one Env
// is reused across Compilers and goroutines. Initialized once via
// filterCELEnvOnce; creation errors are sticky in filterCELEnvErr.
var (
	filterCELEnvOnce sync.Once
	filterCELEnv     *cel.Env
	filterCELEnvErr  error
)

func sharedCELEnv() (*cel.Env, error) {
	filterCELEnvOnce.Do(func() {
		filterCELEnv, filterCELEnvErr = newCELEnv()
	})
	return filterCELEnv, filterCELEnvErr
}

// CompileFilter implements [CELSQLCompiler].
func (c Compiler) CompileFilter(ctx context.Context, in CompileFilterInput) (SQLPredicate, error) {
	if in.Filter == "" {
		return SQLPredicate{SQL: "TRUE"}, nil
	}

	env, err := sharedCELEnv()
	if err != nil {
		return SQLPredicate{}, fmt.Errorf("filter: create CEL environment: %w", err)
	}

	checked, issues := env.Compile(in.Filter)
	if issues != nil && issues.Err() != nil {
		return SQLPredicate{}, fmt.Errorf("filter: %w: %v", domain.ErrInvalidArgument, issues.Err())
	}

	params := c.Params
	if params == nil {
		return SQLPredicate{}, fmt.Errorf("filter: Compiler.Params is required")
	}

	root := checked.NativeRep().Expr()
	var rs *resolveSchema
	if c.Schemas != nil {
		rs = &resolveSchema{provider: c.Schemas}
	}
	st := &state{
		ctx:    ctx,
		fields: c.Fields,
		guard:  hasResourceTypeGuard(root),
		b:      &builder{params: params},
		schema: rs,
	}

	sql, err := compileBool(root, st)
	if err != nil {
		return SQLPredicate{}, err
	}
	return SQLPredicate{SQL: sql, Args: st.b.args}, nil
}

// newCELEnv declares the QueryResources result envelope: the common
// fields as plain strings, plus a single dynamically-typed "resource"
// variable. Declaring resource as cel.DynType lets CEL's checker
// accept any resource.* selection/index syntax without itself
// validating field names -- that validation is the configured
// [FieldResolver]'s job, not cel-go's, since the supported resource.*
// shape depends on the storage backend's row/JSON layout the CEL type
// system knows nothing about.
//
// Called once from sharedCELEnv. EagerlyValidateDeclarations(true)
// forces checker init at construction so concurrent Compile calls on
// the shared Env do not race on lazy checker setup.
func newCELEnv() (*cel.Env, error) {
	// Top-level CEL variables are name and resourceType (canonical
	// ProtoJSON envelope names). Identity components are not declared
	// as top-level variables so CEL rejects them before the field
	// resolver runs; resource.* validation remains the resolver's job.
	return cel.NewEnv(
		cel.EagerlyValidateDeclarations(true),
		cel.ASTValidators(cel.ValidateTimestampLiterals()),
		cel.Variable("name", cel.StringType),
		cel.Variable("resourceType", cel.StringType),
		cel.Variable("resource", cel.DynType),
	)
}

// builder accumulates parameterized SQL args and hands back
// placeholders via the configured [ParamBinder], so every literal
// value compiled from the filter becomes a bind parameter rather
// than inlined SQL text.
type builder struct {
	params ParamBinder
	args   []any
}

func (b *builder) bind(v any) string {
	b.args = append(b.args, v)
	return b.params.Placeholder(len(b.args))
}

// state threads the per-compilation context through compileBool and
// the configured FieldResolver: ctx/guard/schema become part of every
// [ResolveContext], and b provides parameter binding both to the
// compiler itself (literal comparison values) and, via
// [ResolveContext.Bind], to the resolver (e.g. label/condition map
// keys).
type state struct {
	ctx    context.Context
	fields FieldResolver
	// guard is the resourceType literal from a top-level `&&`
	// conjunct `resourceType == "..."`, or nil if there is none. See
	// hasResourceTypeGuard.
	guard  *domain.ResourceType
	b      *builder
	schema *resolveSchema
}

// resolve looks up path's SQL expression through st.fields, building
// the [ResolveContext] every call site needs.
func (st *state) resolve(path FieldPath, hint TypeHint) (SQLExpr, error) {
	if st.fields == nil {
		return SQLExpr{}, fmt.Errorf("filter: %w: field %q: no field resolver configured", domain.ErrInvalidArgument, path)
	}
	return st.fields.Resolve(path, hint, st.resolveContext())
}

func (st *state) resolvePresence(path FieldPath) (string, error) {
	if st.fields == nil {
		return "", fmt.Errorf("filter: %w: field %q: no field resolver configured", domain.ErrInvalidArgument, path)
	}
	return st.fields.ResolvePresence(path, st.resolveContext())
}

func (st *state) resolveMembership(path FieldPath, key string) (string, error) {
	if st.fields == nil {
		return "", fmt.Errorf("filter: %w: field %q: no field resolver configured", domain.ErrInvalidArgument, path)
	}
	return resolveContainerMembership(st.fields, path, key, st.resolveContext())
}

func (st *state) resolveContext() ResolveContext {
	return ResolveContext{
		Context:             st.ctx,
		GuardedResourceType: st.guard,
		Bind:                st.b.bind,
		schema:              st.schema,
	}
}
