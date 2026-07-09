package querysql

import (
	"context"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// FieldPath is a CEL field-select/index chain flattened to its
// segment names, outermost-first -- e.g. resource.labels["team"]
// becomes ["resource", "labels", "team"]; a bare envelope identifier
// like name becomes ["name"]. See fieldPathFromExpr for how a CEL AST
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
// GIN-friendly JSONB containment predicate, and how name/resource_type
// equality can special-case to constituent-column predicates.
//
// In, when non-nil, likewise overrides the generic "SQL IN (...)"
// path -- e.g. a Postgres resolver may rewrite resource_type in
// ["a/T", "b/U"] to (er.service_name, er.type_name) IN (...).
type SQLExpr struct {
	SQL string

	Compare func(op ComparisonOperator, lit any, bind func(any) string) (sql string, handled bool, err error)
	In      func(values []any, bind func(any) string) (sql string, handled bool, err error)
}

// ResolveContext carries the per-compilation state a [FieldResolver]
// needs beyond the field path and type hint themselves.
type ResolveContext struct {
	// Context is QueryResources' call context, threaded through for
	// resolvers that need it (e.g. to call a
	// [domain.QuerySchemaProvider] while validating a type-specific
	// path).
	Context context.Context

	// GuardedResourceType is the resource_type literal from the
	// filter's top-level `resource_type == "..."` conjunct (see
	// hasResourceTypeGuard's doc), or nil if the filter has none.
	// Fields backed by a JSON shape that differs per resource_type
	// (e.g. resource.spec.*) require this to be non-nil.
	GuardedResourceType *domain.ResourceType

	// Bind registers v as a SQL bind parameter and returns the
	// placeholder text produced by the compiler's [ParamBinder].
	// FieldResolver implementations must call this for any
	// filter-supplied *value* they need in the generated SQL -- e.g.
	// a label key from resource.labels["team"] -- rather than writing
	// it into SQL text directly, so a key containing SQL
	// metacharacters can never become part of the query text itself.
	Bind func(v any) string
}

// FieldResolver maps a CEL field path to a SQL expression. This
// package's compiler owns CEL AST lowering -- boolean/comparison
// structure, literals, in, startsWith, parameter binding,
// resource_type guard detection -- and knows about field paths only
// generically; a FieldResolver owns the actual row shape a path reads
// from (column names, JSON extraction, schema-backed path
// validation). See the postgres package's query field resolver for
// this project's current implementation; a future SQLite QueryRepo
// would supply its own.
type FieldResolver interface {
	Resolve(path FieldPath, hint TypeHint, ctx ResolveContext) (SQLExpr, error)
}
