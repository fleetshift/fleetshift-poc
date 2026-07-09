// Package querysql implements the small local CEL-to-SQL adapter
// called for in the QueryRepository POC plan's "cel2sql Adapter"
// section: it compiles a CEL filter, evaluated against
// QueryResources' result envelope, into a parameterized SQL
// predicate.
//
// # Why not github.com/spandigital/cel2sql/v3
//
// The plan calls for evaluating whether a cel2sql-style library fits
// FleetShift's data model before writing a local adapter, without
// naming a specific package (see the plan's "cel2sql Adapter"
// section). github.com/spandigital/cel2sql/v3 (v3.8.8 at evaluation
// time) is the most prominent Go implementation and the one this was
// evaluated against, hands-on rather than assumed. It does not fit.
// That library does target Postgres by default (it is genuinely
// multi-dialect -- Postgres, MySQL, SQLite, DuckDB, BigQuery, Spark --
// with real JSON/JSONB and parameterized-query support), so the
// objection isn't "wrong database". Two concrete incompatibilities
// with this package's required field set surfaced from compiling
// representative filters through it directly:
//
//  1. Map-keyed JSONB access nested under a dynamically-typed parent
//     -- exactly this package's resource.labels["team"],
//     resource.inventory.labels[...], and
//     resource.inventory.conditions["Ready"].status shapes -- does
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
//     resource.spec.*/resource.inventory.observation.* fields are
//     read from a JSON column whose *shape differs by
//     resource_type*, resolved only once resource_type == "..." is
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
// This package owns only CEL AST lowering: boolean/logical structure,
// comparison, "in", and startsWith handling, literal binding, and
// resource_type guard detection (compiler.go). It does not know what
// field paths actually mean -- column names, JSON extraction,
// label/condition map keys, or schema-backed path validation are all
// the concern of whatever [FieldResolver] the caller supplies (see
// field_resolver.go for that contract and the postgres package's
// query_filter.go for this project's Postgres/FleetShift
// implementation). This split exists because querysql's supported CEL
// subset is a QueryResources-wide contract -- any storage backend
// would parse and validate filters the same way -- while the row
// shape a field path resolves to is backend-specific.
//
// Parameter placeholder style is likewise a dialect concern, owned
// by the caller's [ParamBinder] (see param_binder.go). The compiler
// defaults to [DollarParams] (Postgres $N) when Params is nil.
//
// Supported filter shape: see compiler.go for the supported operators
// (&&, ||, !, ==, !=, <, <=, >, >=, in, startsWith) and field-path
// syntax (identifiers, dotted selects, and string-keyed index
// expressions). Anything else -- unsupported operators, arithmetic,
// regex, endsWith/contains/matches, and exists/all/map/filter/has
// macros -- fails closed with [domain.ErrInvalidArgument], as does any
// field path a configured [FieldResolver] doesn't recognize.
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
// cel-go wiring, per the plan's cel2sql adapter guidance.
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
// for concurrent use as long as Fields and Params are (or are
// nil/immutable). CompileFilter shares a package-level *cel.Env (see
// filterCELEnv) whose declarations never change, so concurrent
// Compile calls are safe once that env has been initialized.
type Compiler struct {
	// Fields resolves the field paths a filter references (envelope
	// columns, resource.*, resource.inventory.*, ...) to SQL
	// expressions. A nil Fields is only valid for filters that
	// reference no fields at all (e.g. the empty filter, or a filter
	// built entirely from macros/literals -- both already rejected
	// for other reasons); any real filter compiled against a nil
	// Fields fails with a descriptive error rather than a nil-pointer
	// panic.
	Fields FieldResolver

	// Params formats bind-parameter placeholders in the generated
	// SQL. Nil defaults to [DollarParams] (Postgres $N). SQLite
	// callers should set [QuestionParams] (?N).
	Params ParamBinder
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
		params = DollarParams{}
	}

	root := checked.NativeRep().Expr()
	st := &state{
		ctx:    ctx,
		fields: c.Fields,
		guard:  hasResourceTypeGuard(root),
		b:      &builder{params: params},
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
	// Public CEL envelope fields for this iteration: name and
	// resource_type only. Old POC aliases (kind, platform_name,
	// service_name, api_version, collection_name, resource_id) are
	// intentionally undeclared so CEL rejects them before the field
	// resolver runs; resource.* validation remains the resolver's job.
	return cel.NewEnv(
		cel.EagerlyValidateDeclarations(true),
		cel.Variable("name", cel.StringType),
		cel.Variable("resource_type", cel.StringType),
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
// the configured FieldResolver: ctx/guard become part of every
// [ResolveContext], and b provides parameter binding both to the
// compiler itself (literal comparison values) and, via
// [ResolveContext.Bind], to the resolver (e.g. label/condition map
// keys).
type state struct {
	ctx    context.Context
	fields FieldResolver
	// guard is the resource_type literal from a top-level `&&`
	// conjunct `resource_type == "..."`, or nil if there is none. See
	// hasResourceTypeGuard.
	guard *domain.ResourceType
	b     *builder
}

// resolve looks up path's SQL expression through st.fields, building
// the [ResolveContext] every call site needs.
func (st *state) resolve(path FieldPath, hint TypeHint) (SQLExpr, error) {
	if st.fields == nil {
		return SQLExpr{}, fmt.Errorf("filter: %w: field %q: no field resolver configured", domain.ErrInvalidArgument, path)
	}
	return st.fields.Resolve(path, hint, ResolveContext{
		Context:             st.ctx,
		GuardedResourceType: st.guard,
		Bind:                st.b.bind,
	})
}
