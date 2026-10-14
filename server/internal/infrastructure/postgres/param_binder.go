package postgres

import "fmt"

// dollarParams is the Postgres-style [querysql.ParamBinder]: $1, $2, ...
type dollarParams struct{}

func (dollarParams) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}
