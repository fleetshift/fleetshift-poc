package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsSerializationFailure(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		err := &pgconn.PgError{Code: "40001"}
		if !isSerializationFailure(err) {
			t.Error("want true for SQLSTATE 40001")
		}
	})

	t.Run("wrapped", func(t *testing.T) {
		inner := &pgconn.PgError{Code: "40001"}
		err := fmt.Errorf("commit: %w", inner)
		if !isSerializationFailure(err) {
			t.Error("want true for wrapped 40001")
		}
	})

	t.Run("non-pg error", func(t *testing.T) {
		if isSerializationFailure(errors.New("some other error")) {
			t.Error("want false for non-pg error")
		}
	})

	t.Run("different pg code", func(t *testing.T) {
		err := &pgconn.PgError{Code: "23505"}
		if isSerializationFailure(err) {
			t.Error("want false for unique violation code")
		}
	})

	t.Run("nil", func(t *testing.T) {
		if isSerializationFailure(nil) {
			t.Error("want false for nil")
		}
	})
}
