package postgres_test

import (
	"testing"

	"github.com/stolostron/ocm-resource-manager/server/internal/domain"
	"github.com/stolostron/ocm-resource-manager/server/internal/domain/signerenrollmentrepotest"
	"github.com/stolostron/ocm-resource-manager/server/internal/infrastructure/postgres"
)

func TestSignerEnrollmentRepo(t *testing.T) {
	t.Parallel()
	signerenrollmentrepotest.Run(t, func(t *testing.T) domain.Store {
		return &postgres.Store{DB: postgres.OpenTestDB(t)}
	})
}
