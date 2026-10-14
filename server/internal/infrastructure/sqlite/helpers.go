package sqlite

import (
	"database/sql"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type scanner interface {
	Scan(dest ...any) error
}

func collectRows[T any](rows *sql.Rows, scan func(scanner) (T, error)) ([]T, error) {
	defer rows.Close()
	var items []T
	for rows.Next() {
		item, err := scan(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func isUniqueViolation(err error) bool {
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func nullString(b []byte) sql.NullString {
	if b == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: string(b), Valid: true}
}

func nullGeneration(g *domain.Generation) sql.NullInt64 {
	if g == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*g), Valid: true}
}
