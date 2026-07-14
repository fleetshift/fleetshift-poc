package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

var _ domain.QueryRepository = (*QueryRepo)(nil)

// QueryRepo implements [domain.QueryRepository] for SQLite. It is the
// read model query surface over extension resources -- see
// [domain.QueryRepository]'s doc for why this is not an aggregate
// repository.
//
// Platform aggregate search is intentionally not implemented yet
// (same deferral as the Postgres sibling).
type QueryRepo struct {
	DB *sql.Tx

	// Compiler defaults to querysql.Compiler with this package's
	// queryFieldResolver and questionParams when nil (see compiler()).
	// Overridable for tests that need to exercise QueryResources
	// against a stub compiler; in that case SchemaProvider is ignored
	// since the override owns its own field resolution, if any.
	Compiler querysql.CELSQLCompiler

	// SchemaProvider is threaded into the default compiler's field
	// resolver so resource.spec.*/resource.observation.* field paths
	// can be validated against real descriptors when known, and is
	// also used to scope QueryResources to activated types (see
	// [domain.ResolveQueryResourceTypeScope]). Nil is a valid,
	// permissive default (no activation IN constraint).
	SchemaProvider domain.QuerySchemaProvider
}

func (r *QueryRepo) compiler() querysql.CELSQLCompiler {
	if r.Compiler != nil {
		return r.Compiler
	}
	return querysql.Compiler{
		Fields: queryFieldResolver{SchemaProvider: r.SchemaProvider},
		Params: questionParams{},
	}
}

// queryResourceRow is scanQueryResourceRow's internal result: the
// public [domain.QueryResourceResult] plus the row's type_name, which
// is part of the default ordering/keyset but is not itself exposed as
// a public CEL field (ResourceType already encodes
// service_name/type_name together).
type queryResourceRow struct {
	result   domain.QueryResourceResult
	typeName string
}

func (r *QueryRepo) QueryResources(ctx context.Context, req domain.QueryResourcesRequest) (domain.QueryResourcesPage, error) {
	order, err := resolveQueryOrder(req.OrderBy)
	if err != nil {
		return domain.QueryResourcesPage{}, err
	}

	scope, err := domain.ResolveQueryResourceTypeScope(ctx, r.SchemaProvider, req.Filter)
	if err != nil {
		return domain.QueryResourcesPage{}, err
	}

	// Compile before honoring an empty activation scope so invalid
	// filters (deprecated paths, unsupported operators) still fail
	// closed instead of returning a silent empty page.
	predicate, err := r.compiler().CompileFilter(ctx, querysql.CompileFilterInput{Filter: req.Filter})
	if err != nil {
		return domain.QueryResourcesPage{}, err
	}
	if scope.Empty {
		return domain.QueryResourcesPage{}, nil
	}

	limit := clampQueryPageSize(req.PageSize)

	var keyset *queryPageToken
	if req.PageToken != "" {
		tok, err := decodeQueryPageToken(req.PageToken, req.Filter, req.OrderBy, scope.Types)
		if err != nil {
			return domain.QueryResourcesPage{}, err
		}
		keyset = &tok
	}

	predicateSQL := predicate.SQL
	args := append([]any{}, predicate.Args...)

	typeSQL, args, err := resourceTypesPredicateSQL(scope.Types, args)
	if err != nil {
		return domain.QueryResourcesPage{}, err
	}
	if typeSQL != "TRUE" {
		predicateSQL = "(" + predicateSQL + ") AND (" + typeSQL + ")"
	}

	keysetSQL := "TRUE"
	if keyset != nil {
		keysetSQL, args = keysetPredicateSQL(order, *keyset, args)
	}

	// Fetch one extra row so we can tell whether a NextPageToken is
	// warranted without a second round trip. LIMIT uses ?N (bound
	// last) so it stays consistent with numbered filter/keyset binds
	// -- see buildQueryResourcesSQL.
	limitPlaceholder := len(args) + 1
	args = append(args, limit+1)

	query := buildQueryResourcesSQL(predicateSQL, keysetSQL, order, limitPlaceholder)
	rows, err := r.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return domain.QueryResourcesPage{}, fmt.Errorf("query resources: %w", err)
	}
	scanned, err := collectRows(rows, scanQueryResourceRow)
	if err != nil {
		return domain.QueryResourcesPage{}, fmt.Errorf("query resources: %w", err)
	}

	var page domain.QueryResourcesPage
	if len(scanned) > limit {
		scanned = scanned[:limit]
		last := scanned[len(scanned)-1]
		tok, err := encodeQueryPageToken(queryPageToken{
			Version:        queryPageTokenVersion,
			FilterHash:     queryFilterHash(req.Filter, req.OrderBy, scope.Types),
			OrderBy:        req.OrderBy,
			CollectionName: string(last.result.CollectionName),
			ResourceID:     string(last.result.ResourceID),
			ServiceName:    string(last.result.ServiceName),
			TypeName:       last.typeName,
		})
		if err != nil {
			return domain.QueryResourcesPage{}, fmt.Errorf("query resources: %w", err)
		}
		page.NextPageToken = tok
	}

	page.Resources = make([]domain.QueryResourceResult, len(scanned))
	for i, row := range scanned {
		page.Resources[i] = row.result
	}
	return page, nil
}

// scanQueryResourceRow scans one row of buildQueryResourcesSQL's
// final SELECT: the extension envelope ordering columns plus the full
// extension projection columns from erViewQuerySQLite. It builds the
// extension read model by delegating to
// extensionResourceViewFromColumns -- the same construction logic
// [ExtensionResourceRepo.GetView] uses -- so the projection stays
// provably equivalent to that read (see queryrepotest's equivalence
// tests) without a second, per-row database round trip.
func scanQueryResourceRow(s scanner) (queryResourceRow, error) {
	var collectionName, resourceID, serviceName, typeName string

	var uidStr, extServiceName, extTypeName, extCollectionName, extResourceID string
	var extLabels, extReportedAliases string
	var extCreatedAt, extUpdatedAt string
	var extCurrentVersion sql.NullInt64
	var extFulfillmentID sql.NullString
	var riSpec, riCreatedAt sql.NullString
	var fCols nullableFulfillmentScanColumns
	var invLabels, invObservation sql.NullString
	var invObservedAt, invUpdatedAt sql.NullString
	var invConditionsJSON sql.NullString

	if err := s.Scan(append(append([]any{
		&collectionName, &resourceID, &serviceName, &typeName,

		&uidStr, &extServiceName, &extTypeName, &extCollectionName, &extResourceID, &extLabels, &extReportedAliases,
		&extCreatedAt, &extUpdatedAt,
		&extCurrentVersion, &extFulfillmentID,
		&riSpec, &riCreatedAt,
	}, fCols.dests()...),
		&invLabels, &invObservation, &invObservedAt, &invUpdatedAt, &invConditionsJSON,
	)...); err != nil {
		return queryResourceRow{}, fmt.Errorf("scan query resource row: %w", err)
	}

	uid, err := domain.ParseExtensionResourceUID(uidStr)
	if err != nil {
		return queryResourceRow{}, fmt.Errorf("query resources: parse uid: %w", err)
	}

	view, err := extensionResourceViewFromColumns(
		uid, extServiceName, extTypeName, extCollectionName, extResourceID,
		extLabels, extReportedAliases,
		extCreatedAt, extUpdatedAt,
		extCurrentVersion, extFulfillmentID,
		riSpec, riCreatedAt,
		fCols,
		invLabels, invObservation, invObservedAt, invUpdatedAt,
		invConditionsJSON,
	)
	if err != nil {
		return queryResourceRow{}, fmt.Errorf("query resources: build extension view: %w", err)
	}

	rt := domain.ResourceType(serviceName + "/" + typeName)
	name := string(domain.NewFullResourceName(domain.ServiceName(serviceName),
		domain.ResourceName(collectionName+"/"+resourceID)))

	result := domain.QueryResourceResult{
		Kind:           domain.QueryResourceKindExtension,
		Name:           name,
		ResourceType:   rt,
		ServiceName:    domain.ServiceName(serviceName),
		CollectionName: domain.CollectionName(collectionName),
		ResourceID:     domain.ResourceID(resourceID),
		Platform:       nil,
		Extension:      &view,
	}

	return queryResourceRow{result: result, typeName: typeName}, nil
}

// resourceTypesPredicateSQL ANDs an IN constraint on
// (er.service_name, er.type_name) when types is non-empty. Nil/empty
// types means no extra constraint (TRUE) -- used when
// [domain.QuerySchemaProvider] is nil. Malformed ResourceType values
// return [domain.ErrInvalidArgument].
func resourceTypesPredicateSQL(types []domain.ResourceType, args []any) (string, []any, error) {
	if len(types) == 0 {
		return "TRUE", args, nil
	}
	tuples := make([]string, 0, len(types))
	for _, rt := range types {
		parsed, err := domain.ParseResourceType(string(rt))
		if err != nil {
			return "", nil, fmt.Errorf("resource_types: %w: %v", domain.ErrInvalidArgument, err)
		}
		args = append(args, string(parsed.ServiceName()), parsed.TypeName())
		n := len(args)
		tuples = append(tuples, fmt.Sprintf("(?%d, ?%d)", n-1, n))
	}
	return fmt.Sprintf("(er.service_name, er.type_name) IN (%s)", strings.Join(tuples, ", ")), args, nil
}
