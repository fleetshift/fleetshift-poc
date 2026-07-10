package postgres

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// defaultQueryPageSize/maxQueryPageSize implement
// [domain.QueryResourcesRequest.PageSize]'s documented default/max.
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

// queryOrderMode is one of the small explicit orderings QueryResources
// supports. Arbitrary order_by expressions are rejected.
type queryOrderMode string

const (
	// queryOrderDefault groups by logical resource identity first
	// (collection_name, resource_id), then by addon type. Backed by
	// idx_extension_resources_query_order.
	queryOrderDefault queryOrderMode = ""
	// queryOrderResourceTypeName groups by resource type first, then
	// by relative resource identity. Requested as "resource_type,name".
	// Backed by idx_extension_resources_type_query_order.
	queryOrderResourceTypeName queryOrderMode = "resource_type,name"
)

// querySupportedOrder describes one supported ORDER BY / keyset shape.
type querySupportedOrder struct {
	Mode queryOrderMode

	// OrderBySQL is the unqualified ORDER BY clause used inside
	// filtered_page (columns are already scoped to that CTE).
	OrderBySQL string
	// OrderBySQLQualified is the same ordering re-applied, fp-qualified,
	// on the final SELECT after LATERAL hydration.
	OrderBySQLQualified string
	// CursorColumns are the er-qualified columns the keyset predicate
	// compares, in order.
	CursorColumns []string
}

var (
	queryOrderDefaultSpec = querySupportedOrder{
		Mode:                queryOrderDefault,
		OrderBySQL:          "collection_name, resource_id, service_name, type_name",
		OrderBySQLQualified: "fp.collection_name, fp.resource_id, fp.service_name, fp.type_name",
		CursorColumns: []string{
			"er.collection_name",
			"er.resource_id",
			"er.service_name",
			"er.type_name",
		},
	}
	queryOrderResourceTypeNameSpec = querySupportedOrder{
		Mode:                queryOrderResourceTypeName,
		OrderBySQL:          "service_name, type_name, collection_name, resource_id",
		OrderBySQLQualified: "fp.service_name, fp.type_name, fp.collection_name, fp.resource_id",
		CursorColumns: []string{
			"er.service_name",
			"er.type_name",
			"er.collection_name",
			"er.resource_id",
		},
	}
)

// resolveQueryOrder maps a request OrderBy string to a supported
// order. Empty selects the default. Unsupported values return
// [domain.ErrInvalidArgument].
func resolveQueryOrder(orderBy string) (querySupportedOrder, error) {
	switch queryOrderMode(strings.TrimSpace(orderBy)) {
	case queryOrderDefault:
		return queryOrderDefaultSpec, nil
	case queryOrderResourceTypeName:
		return queryOrderResourceTypeNameSpec, nil
	default:
		return querySupportedOrder{}, fmt.Errorf("order_by: %w: unsupported ordering %q (supported: \"\", %q)",
			domain.ErrInvalidArgument, orderBy, queryOrderResourceTypeName)
	}
}

// keysetPredicateSQL builds the row-wise "(cols...) > ($N, ...)"
// predicate for order, appending the cursor values from tok onto args.
// Returns the SQL fragment and the extended args slice.
func keysetPredicateSQL(order querySupportedOrder, tok queryPageToken, args []any) (string, []any) {
	placeholders := make([]string, len(order.CursorColumns))
	for i := range order.CursorColumns {
		placeholders[i] = fmt.Sprintf("$%d", len(args)+1+i)
	}
	sql := fmt.Sprintf("(%s) > (%s)",
		strings.Join(order.CursorColumns, ", "),
		strings.Join(placeholders, ", "))

	// Cursor values follow CursorColumns order for each mode.
	switch order.Mode {
	case queryOrderResourceTypeName:
		args = append(args, tok.ServiceName, tok.TypeName, tok.CollectionName, tok.ResourceID)
	default:
		args = append(args, tok.CollectionName, tok.ResourceID, tok.ServiceName, tok.TypeName)
	}
	return sql, args
}

// Bumped from 1: the previous POC token carried kind and used a
// different default order. Existing tokens are intentionally not
// compatible.
const queryPageTokenVersion = 2

// queryPageToken is the opaque page token payload for QueryResources
// keyset pagination. Each cursor field mirrors one ordering column so
// a token's keyset can drive a row-wise "(cols...) > (vals...)"
// predicate directly. OrderBy records the selected supported order
// mode (empty for default).
type queryPageToken struct {
	Version        int    `json:"version"`
	FilterHash     string `json:"filter_hash"`
	OrderBy        string `json:"order_by"`
	CollectionName string `json:"collection_name"`
	ResourceID     string `json:"resource_id"`
	ServiceName    string `json:"service_name"`
	TypeName       string `json:"type_name"`
}

// queryFilterHash hashes the filter/order_by/activated-types triple a
// page token was minted against, so a token replayed against a
// different filter, ordering, or activation scope fails closed instead
// of silently resuming a different query's keyset with stale
// semantics. resourceTypes is the effective activation scope from
// [domain.ResolveQueryResourceTypeScope] (nil when no provider).
func queryFilterHash(filter, orderBy string, resourceTypes []domain.ResourceType) string {
	h := sha256.New()
	h.Write([]byte(filter))
	h.Write([]byte{0})
	h.Write([]byte(orderBy))
	h.Write([]byte{0})
	for _, rt := range resourceTypes {
		h.Write([]byte(rt))
		h.Write([]byte{0})
	}
	sum := h.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(sum)
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
// filter/order_by/activation-scope of the current request. Any
// structural problem or filter/order_by/type-scope mismatch is
// [domain.ErrInvalidArgument]: the request that minted the token
// wasn't necessarily malformed, but resuming it against a different
// query is a precondition violation the caller must fix, not a
// retryable server condition.
func decodeQueryPageToken(raw, filter, orderBy string, resourceTypes []domain.ResourceType) (queryPageToken, error) {
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
	if tok.FilterHash != queryFilterHash(filter, orderBy, resourceTypes) {
		return queryPageToken{}, fmt.Errorf("page_token: %w: does not match the current filter/order_by", domain.ErrInvalidArgument)
	}
	return tok, nil
}
