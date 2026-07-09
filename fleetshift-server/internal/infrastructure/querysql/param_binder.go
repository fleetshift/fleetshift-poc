package querysql

import "fmt"

// ParamBinder formats SQL bind-parameter placeholders. The compiler
// accumulates argument values in order and asks the binder for the
// placeholder text that should appear in the generated SQL for each
// 1-based index. Implementations must be safe for concurrent use
// (they are typically pure functions of the index).
//
// This is the dialect seam for parameter style: Postgres uses
// [DollarParams] ($1, $2, ...); SQLite uses [QuestionParams]
// (?1, ?2, ...). Numbered forms matter when a FieldResolver embeds
// one bound placeholder into an expression that is later repeated
// (e.g. SQLite's safe JSON casts): a bare "?" would consume a fresh
// positional slot on every occurrence. Field resolvers never see the
// binder directly -- they call [ResolveContext.Bind], which already
// applies the compiler's configured ParamBinder.
type ParamBinder interface {
	// Placeholder returns the SQL text for the 1-based parameter
	// index n. n is always >= 1.
	Placeholder(n int) string
}

// DollarParams is the Postgres-style ParamBinder: $1, $2, ...
type DollarParams struct{}

// Placeholder implements [ParamBinder].
func (DollarParams) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}

// QuestionParams is the SQLite-style ParamBinder: ?1, ?2, ...
// Numbered (not bare "?") so a single Bind can be referenced from
// multiple occurrences of the same SQL expression -- required when
// SQLite field resolvers wrap a parameterized json_extract in a
// CASE that repeats the expression.
type QuestionParams struct{}

// Placeholder implements [ParamBinder].
func (QuestionParams) Placeholder(n int) string {
	return fmt.Sprintf("?%d", n)
}
