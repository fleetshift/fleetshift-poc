package querysql

import (
	"context"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// FieldPath is a CEL field-select/index chain flattened to its
// segment names, outermost-first -- e.g. resource.labels["team"] and
// resource.labels.team both become ["resource", "labels", "team"]; a
// bare envelope identifier like name becomes ["name"]. Select and
// string-index syntax with the same raw key are equivalent; segment
// kind is not retained. See fieldPathFromExpr for how a CEL AST
// expression becomes a FieldPath.
type FieldPath struct {
	Segments []string
}

// String returns the dot-joined path, for error messages.
func (p FieldPath) String() string { return strings.Join(p.Segments, ".") }

// TypeHint tells a [FieldResolver] what SQL value shape the
// surrounding comparison expects, since CEL's own type system doesn't
// know a field's real SQL shape (e.g. resource.generation is a plain
// CEL int whether it's backed by a native integer column or
// JSON-extracted text). The compiler derives it from the *other* side
// of a comparison/in -- e.g. `resource.generation > 4` derives
// TypeHintNumber from the literal 4 -- so a resolver backing a field
// with JSON-extracted text (see the postgres field resolver's
// jsonTextField) knows what to cast it to.
type TypeHint int

const (
	TypeHintUnknown TypeHint = iota
	TypeHintString
	TypeHintBool
	TypeHintNumber
	// TypeHintTimestamp is used when both sides of a comparison are
	// timestamp() conversions (CEL timestamps), not ProtoJSON strings.
	TypeHintTimestamp
)

// ComparisonOperator is the named comparison the compiler asks a
// [SQLExpr.Compare] hook to handle. Using a named type (rather than
// raw SQL operator strings) keeps the FleetShift field resolver from
// depending on the compiler's SQL spelling of each operator.
type ComparisonOperator int

const (
	OpEqual ComparisonOperator = iota
	OpNotEqual
	OpLess
	OpLessEqual
	OpGreater
	OpGreaterEqual
)

// SQLExpr is a field path resolved to a SQL expression.
//
// Compare, when non-nil, lets a field mapping override the generic
// "SQL op <bound literal>" compilation for a specific comparison.
// Returning handled=false falls back to the generic path. This is how
// a Postgres resolver turns resource.labels["k"] == "v" into a
// GIN-friendly JSONB containment predicate, and how name/resourceType
// equality can special-case to constituent-column predicates.
//
// In, when non-nil, likewise overrides the generic "SQL IN (...)"
// path -- e.g. a Postgres resolver may rewrite resourceType in
// ["a/T", "b/U"] to (er.service_name, er.type_name) IN (...).
//
// StartsWith, when non-nil, overrides the generic
// "SQL LIKE <escaped prefix>% ESCAPE ..." path for
// field.startsWith("prefix"). Resolvers use this for field-specific
// case folding (e.g. lowercasing the prefix for stored-lowercase
// columns) or dialect-specific prefix predicates; handled=false keeps
// the generic LIKE.
type SQLExpr struct {
	SQL string

	Compare    func(op ComparisonOperator, lit any, bind func(any) string) (sql string, handled bool, err error)
	In         func(values []any, bind func(any) string) (sql string, handled bool, err error)
	StartsWith func(prefix string, bind func(any) string) (sql string, handled bool, err error)
}

// ResolveContext carries the per-compilation state a [FieldResolver]
// needs beyond the field path and type hint themselves, plus
// dialect-neutral schema operations owned by this package.
type ResolveContext struct {
	// Context is QueryResources' call context, threaded through for
	// resolvers and schema lookups.
	Context context.Context

	// GuardedResourceType is the resourceType literal from the
	// filter's top-level `resourceType == "..."` conjunct (see
	// hasResourceTypeGuard's doc), or nil if the filter has none.
	// When non-nil, [ValidateSpecPath] / [ValidateObservationPath] /
	// [ClassifyResourceContainer] may validate type-shaped paths
	// against that type's schema. A guard is not required to compile
	// those paths.
	GuardedResourceType *domain.ResourceType

	// Bind registers v as a SQL bind parameter and returns the
	// placeholder text produced by the compiler's [ParamBinder].
	// FieldResolver implementations must call this for any
	// filter-supplied *value* they need in the generated SQL -- e.g.
	// a label key from resource.labels["team"] -- rather than writing
	// it into SQL text directly, so a key containing SQL
	// metacharacters can never become part of the query text itself.
	Bind func(v any) string

	// schema is the per-compilation lazy lookup cell (nil when the
	// compiler has no SchemaProvider). Copied ResolveContexts share
	// the same *resolveSchema.
	schema *resolveSchema
}

// FieldResolver maps a CEL field path to a SQL expression. This
// package's compiler owns CEL AST lowering -- boolean/comparison
// structure, literals, in, startsWith, has / presence, container
// membership orchestration, parameter binding, resourceType guard
// detection, and dialect-neutral ProtoJSON schema validation /
// container classification -- and knows about field paths only
// generically. A FieldResolver owns the actual row shape a path reads
// from (column names, JSON extraction, projection-faithful presence,
// and list/unknown JSON membership SQL). The postgres and sqlite
// packages each supply a QueryResources FieldResolver for their row
// shapes.
//
// Presence and container membership are first-class operations, not
// "SQL IS NOT NULL" over [Resolve]'s value expression: ProtoJSON
// default omission means a non-NULL storage column can still be
// absent from the projected response body.
//
// Object-key container membership is orchestrated in this package and
// always lowered through [ResolvePresence] of path+key when both
// spellings are supported. Dialects only render list and runtime
// object/array dispatch via [ResolveJSONMembership].
type FieldResolver interface {
	Resolve(path FieldPath, hint TypeHint, ctx ResolveContext) (SQLExpr, error)

	// ResolvePresence compiles a CEL has(path) / test-only select into
	// a SQL boolean predicate that is true iff path is present on the
	// projected ProtoJSON resource body for reachable writer shapes.
	ResolvePresence(path FieldPath, ctx ResolveContext) (sql string, err error)

	// ResolveJSONMembership compiles list or runtime-dispatch
	// container membership for a classified spec/observation JSON
	// target. target.Kind is [ContainerKindList] or
	// [ContainerKindUnknown] only; Object/Scalar membership never
	// reaches this method.
	ResolveJSONMembership(target JSONMembershipTarget, key string, ctx ResolveContext) (sql string, err error)
}
