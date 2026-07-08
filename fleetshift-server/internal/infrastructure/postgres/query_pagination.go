package postgres

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// defaultQueryPageSize/maxQueryPageSize implement
// [domain.QueryResourcesRequest.PageSize]'s documented default/max
// (see the QueryRepository POC plan's "Default Ordering and
// Pagination" section).
const (
	defaultQueryPageSize = 50
	maxQueryPageSize     = 500
)

// clampQueryPageSize applies the default/max above: non-positive
// requests fall back to the default; oversized requests clamp to the
// max.
func clampQueryPageSize(requested int32) int {
	if requested <= 0 {
		return defaultQueryPageSize
	}
	if int(requested) > maxQueryPageSize {
		return maxQueryPageSize
	}
	return int(requested)
}

// queryResultsOrderBy is QueryResources' deterministic default
// ordering, applied inside filtered_page (unqualified: that CTE's
// WHERE/ORDER BY only ever sees all_rows' columns, so there is no
// ambiguity to qualify away). It is also, column-for-column, the
// keyset [queryPageToken] encodes.
const queryResultsOrderBy = "kind, service_name, collection_name, resource_id, type_name"

// queryResultsOrderByQualified is the same ordering re-applied,
// fp-qualified, on buildQueryResourcesSQL's final SELECT. CTE row
// order is not a guarantee Postgres documents once the plan adds the
// LEFT JOIN LATERALs, so the outer query re-sorts explicitly rather
// than relying on filtered_page's internal ordering surviving the
// join. It must be qualified here because extv/plat's LATERAL output
// also has columns like service_name that would otherwise collide.
const queryResultsOrderByQualified = "fp.kind, fp.service_name, fp.collection_name, fp.resource_id, fp.type_name"

const queryPageTokenVersion = 1

// queryPageToken is the opaque page token payload for QueryResources
// keyset pagination. Each field mirrors one queryResultsOrderBy
// column, in order, so a token's keyset can drive a row-wise
// "(cols...) > (vals...)" predicate directly.
type queryPageToken struct {
	Version        int    `json:"version"`
	FilterHash     string `json:"filter_hash"`
	OrderBy        string `json:"order_by"`
	Kind           string `json:"kind"`
	ServiceName    string `json:"service_name"`
	CollectionName string `json:"collection_name"`
	ResourceID     string `json:"resource_id"`
	TypeName       string `json:"type_name"`
}

// queryFilterHash hashes the filter/order_by pair a page token was
// minted against, so a token replayed against a different filter or
// ordering fails closed instead of silently resuming a different
// query's keyset with stale semantics.
func queryFilterHash(filter, orderBy string) string {
	sum := sha256.Sum256([]byte(filter + "\x00" + orderBy))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// encodeQueryPageToken encodes tok as opaque base64url JSON.
func encodeQueryPageToken(tok queryPageToken) (string, error) {
	data, err := json.Marshal(tok)
	if err != nil {
		return "", fmt.Errorf("encode page token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

// decodeQueryPageToken decodes and validates a page token against the
// filter/order_by of the current request. Any structural problem or
// filter/order_by mismatch is [domain.ErrInvalidArgument]: the
// request that minted the token wasn't necessarily malformed, but
// resuming it against a different query is a precondition violation
// the caller must fix, not a retryable server condition.
func decodeQueryPageToken(raw, filter, orderBy string) (queryPageToken, error) {
	var tok queryPageToken
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return queryPageToken{}, fmt.Errorf("page_token: %w: malformed encoding", domain.ErrInvalidArgument)
	}
	if err := json.Unmarshal(data, &tok); err != nil {
		return queryPageToken{}, fmt.Errorf("page_token: %w: malformed payload", domain.ErrInvalidArgument)
	}
	if tok.Version != queryPageTokenVersion {
		return queryPageToken{}, fmt.Errorf("page_token: %w: unsupported version %d", domain.ErrInvalidArgument, tok.Version)
	}
	if tok.FilterHash != queryFilterHash(filter, orderBy) {
		return queryPageToken{}, fmt.Errorf("page_token: %w: does not match the current filter/order_by", domain.ErrInvalidArgument)
	}
	return tok, nil
}
