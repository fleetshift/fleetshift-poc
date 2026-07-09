package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

var _ domain.QueryRepository = (*QueryRepo)(nil)

// QueryRepo implements [domain.QueryRepository] for Postgres. It is
// the read model query surface over extension resources -- see
// [domain.QueryRepository]'s doc for why this is not an aggregate
// repository.
//
// TODO: do not restore platform aggregate search by re-adding the
// old platform_rows CTE union in buildQueryResourcesSQL. Platform
// rows are reserved for a future identity/query model with its own
// indexes; a derived approximation over extension_resources is what
// this iteration deliberately removed.
type QueryRepo struct {
	DB *sql.Tx

	// Compiler defaults to querysql.Compiler with this package's
	// queryFieldResolver and [querysql.DollarParams] when nil (see
	// compiler()). Overridable for tests that need to exercise
	// QueryResources against a stub compiler; in that case
	// SchemaProvider is ignored since the override owns its own field
	// resolution, if any.
	Compiler querysql.CELSQLCompiler

	// SchemaProvider is threaded into the default compiler's field
	// resolver so resource.spec.*/resource.inventory.observation.*
	// field paths can be validated against real descriptors when
	// known (see [domain.QuerySchemaProvider]'s doc). Nil is a valid,
	// permissive default.
	SchemaProvider domain.QuerySchemaProvider
}

func (r *QueryRepo) compiler() querysql.CELSQLCompiler {
	if r.Compiler != nil {
		return r.Compiler
	}
	return querysql.Compiler{
		Fields: queryFieldResolver{SchemaProvider: r.SchemaProvider},
		Params: querysql.DollarParams{},
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

	limit := clampQueryPageSize(req.PageSize)

	var keyset *queryPageToken
	if req.PageToken != "" {
		tok, err := decodeQueryPageToken(req.PageToken, req.Filter, req.OrderBy)
		if err != nil {
			return domain.QueryResourcesPage{}, err
		}
		keyset = &tok
	}

	predicate, err := r.compiler().CompileFilter(ctx, querysql.CompileFilterInput{Filter: req.Filter})
	if err != nil {
		return domain.QueryResourcesPage{}, err
	}
	predicateSQL := predicate.SQL
	args := append([]any{}, predicate.Args...)

	keysetSQL := "TRUE"
	if keyset != nil {
		keysetSQL, args = keysetPredicateSQL(order, *keyset, args)
	}

	// Fetch one extra row so we can tell whether a NextPageToken is
	// warranted without a second round trip.
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
			FilterHash:     queryFilterHash(req.Filter, req.OrderBy),
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
// extension projection columns from erViewQueryPG. It builds the
// extension read model by delegating to
// extensionResourceViewFromColumns -- the same construction logic
// [ExtensionResourceRepo.GetView] uses -- so the projection stays
// provably equivalent to that read (see queryrepotest's equivalence
// tests) without a second, per-row database round trip.
func scanQueryResourceRow(s scanner) (queryResourceRow, error) {
	var collectionName, resourceID, serviceName, typeName string

	var extUID domain.ExtensionResourceUID
	var extServiceName, extTypeName, extCollectionName, extResourceID string
	var extLabels, extReportedAliases string
	var extCreatedAt, extUpdatedAt time.Time
	var extCurrentVersion sql.NullInt64
	var extFulfillmentID sql.NullString
	var riSpec, riCreatedAt sql.NullString
	var fID sql.NullString
	var msVer sql.NullInt64
	var msSpec sql.NullString
	var psVer sql.NullInt64
	var psSpec sql.NullString
	var rsVer sql.NullInt64
	var rsSpec sql.NullString
	var rtJSON, stateStr, pauseReason, statusReason, authJSON sql.NullString
	var provJSON, attestRefJSON sql.NullString
	var generation, observedGeneration, activeWorkflowGen sql.NullInt64
	var fCreatedAt, fUpdatedAt sql.NullString
	var invLabels, invObservation sql.NullString
	var invObservedAt, invUpdatedAt *time.Time
	var invConditionsJSON sql.NullString

	if err := s.Scan(
		&collectionName, &resourceID, &serviceName, &typeName,

		&extUID, &extServiceName, &extTypeName, &extCollectionName, &extResourceID, &extLabels, &extReportedAliases,
		&extCreatedAt, &extUpdatedAt,
		&extCurrentVersion, &extFulfillmentID,
		&riSpec, &riCreatedAt,
		&fID, &msVer, &msSpec, &psVer, &psSpec, &rsVer, &rsSpec,
		&rtJSON, &stateStr, &pauseReason, &statusReason, &authJSON, &provJSON, &attestRefJSON,
		&generation, &observedGeneration, &activeWorkflowGen,
		&fCreatedAt, &fUpdatedAt,
		&invLabels, &invObservation, &invObservedAt, &invUpdatedAt, &invConditionsJSON,
	); err != nil {
		return queryResourceRow{}, fmt.Errorf("scan query resource row: %w", err)
	}

	view, err := extensionResourceViewFromColumns(
		extUID, extServiceName, extTypeName, extCollectionName, extResourceID,
		extLabels, extReportedAliases,
		extCreatedAt, extUpdatedAt,
		extCurrentVersion, extFulfillmentID,
		riSpec, riCreatedAt,
		fID, msVer, msSpec, psVer, psSpec, rsVer, rsSpec,
		rtJSON, stateStr, pauseReason, statusReason, authJSON, provJSON, attestRefJSON,
		generation, observedGeneration, activeWorkflowGen,
		fCreatedAt, fUpdatedAt,
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
