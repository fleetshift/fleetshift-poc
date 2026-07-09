package sqlite

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

var _ domain.QueryRepository = (*QueryRepo)(nil)

// QueryRepo is the SQLite [domain.QueryRepository] stub. Postgres
// implements the extension-only query surface first; SQLite returns
// [domain.ErrUnimplemented] in this iteration. A follow-up can
// translate the supported CEL subset to SQLite's JSON functions and
// add any SQLite-side indexes then; until that lands, SQLite does not
// mirror the Postgres QueryResources indexes.
type QueryRepo struct{}

func (r *QueryRepo) QueryResources(ctx context.Context, req domain.QueryResourcesRequest) (domain.QueryResourcesPage, error) {
	return domain.QueryResourcesPage{}, fmt.Errorf("sqlite query repository: %w", domain.ErrUnimplemented)
}
