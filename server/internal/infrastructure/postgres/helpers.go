package postgres

import (
	"database/sql"
	"errors"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/jackc/pgx/v5/pgconn"
)

type scanner interface {
	Scan(dest ...any) error
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
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

func nullStringFromBytes(b []byte) sql.NullString {
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
