package querysql

// ParamBinder formats SQL bind-parameter placeholders. The compiler
// accumulates argument values in order and asks the binder for the
// placeholder text that should appear in the generated SQL for each
// 1-based index. Implementations must be safe for concurrent use
// (they are typically pure functions of the index).
//
// This is the dialect seam for parameter style: each storage backend
// supplies its own ParamBinder (Postgres $N, SQLite ?N, …). Numbered
// forms matter when a FieldResolver embeds one bound placeholder into
// an expression that is later repeated (e.g. SQLite's safe JSON
// casts): a bare "?" would consume a fresh positional slot on every
// occurrence. Field resolvers never see the binder directly — they
// call [ResolveContext.Bind], which already applies the compiler's
// configured ParamBinder.
type ParamBinder interface {
	// Placeholder returns the SQL text for the 1-based parameter
	// index n. n is always >= 1.
	Placeholder(n int) string
}
