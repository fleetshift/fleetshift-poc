package sqlite

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

var _ domain.QueryRepository = (*QueryRepo)(nil)

// QueryRepo is the SQLite [domain.QueryRepository] stub. Postgres
// implements the query surface first; SQLite returns
// [domain.ErrUnimplemented] until a follow-up translates the
// supported CEL subset to SQLite's JSON functions (see the
// QueryRepository POC plan's "Later SQLite implementation" section).
type QueryRepo struct{}

func (r *QueryRepo) QueryResources(ctx context.Context, req domain.QueryResourcesRequest) (domain.QueryResourcesPage, error) {
	return domain.QueryResourcesPage{}, fmt.Errorf("sqlite query repository: %w", domain.ErrUnimplemented)
}
