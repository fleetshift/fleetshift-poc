package sqlite

import "fmt"

// questionParams is the SQLite-style [querysql.ParamBinder]: ?1, ?2, ...
// Numbered (not bare "?") so a single Bind can be referenced from
// multiple occurrences of the same SQL expression — required when
// field resolvers wrap a parameterized json_extract in a CASE that
// repeats the expression.
type questionParams struct{}

func (questionParams) Placeholder(n int) string {
	return fmt.Sprintf("?%d", n)
}
