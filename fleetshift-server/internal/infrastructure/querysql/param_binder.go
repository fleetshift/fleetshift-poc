package querysql

import "fmt"

// ParamBinder formats SQL bind-parameter placeholders. The compiler
// accumulates argument values in order and asks the binder for the
// placeholder text that should appear in the generated SQL for each
// 1-based index. Implementations must be safe for concurrent use
// (they are typically pure functions of the index).
//
// This is the dialect seam for parameter style: Postgres uses
// [DollarParams] ($1, $2, ...); SQLite's database/sql driver uses
// [QuestionParams] (?, ?, ...). Field resolvers never see the binder
// directly -- they call [ResolveContext.Bind], which already applies
// the compiler's configured ParamBinder.
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

// QuestionParams is the SQLite / database/sql-style ParamBinder: a
// bare "?" for every parameter (positional, not numbered).
type QuestionParams struct{}

// Placeholder implements [ParamBinder].
func (QuestionParams) Placeholder(int) string {
	return "?"
}
