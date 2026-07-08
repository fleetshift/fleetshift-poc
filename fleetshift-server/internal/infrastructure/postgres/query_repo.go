package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres/querysql"
)

var _ domain.QueryRepository = (*QueryRepo)(nil)

// QueryRepo implements [domain.QueryRepository] for Postgres. It is
// the read model query surface spanning platform and extension
// resources -- see [domain.QueryRepository]'s doc for why this is not
// an aggregate repository.
type QueryRepo struct {
	DB *sql.Tx

	// Compiler defaults to querysql.Compiler{Fields:
	// queryFieldResolver{SchemaProvider: SchemaProvider}} when nil
	// (see compiler()). Overridable for tests that need to exercise
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
	return querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: r.SchemaProvider}}
}

// queryResourceRow is scanQueryResourceRow's internal result: the
// public [domain.QueryResourceResult] plus the row's type_name, which
// is part of the default ordering/keyset (see queryResultsOrderBy)
// but is not itself exposed on the public DTO (ResourceType already
// encodes service_name/type_name together for extension rows, and
// platform rows have no type_name yet).
type queryResourceRow struct {
	result   domain.QueryResourceResult
	typeName string
}

func (r *QueryRepo) QueryResources(ctx context.Context, req domain.QueryResourcesRequest) (domain.QueryResourcesPage, error) {
	if req.OrderBy != "" {
		return domain.QueryResourcesPage{}, fmt.Errorf("order_by: %w: custom ordering is not implemented", domain.ErrUnimplemented)
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
		keysetSQL = fmt.Sprintf(
			"(kind, service_name, collection_name, resource_id, type_name) > ($%d, $%d, $%d, $%d, $%d)",
			len(args)+1, len(args)+2, len(args)+3, len(args)+4, len(args)+5,
		)
		args = append(args, keyset.Kind, keyset.ServiceName, keyset.CollectionName, keyset.ResourceID, keyset.TypeName)
	}

	// Fetch one extra row so we can tell whether a NextPageToken is
	// warranted without a second round trip.
	limitPlaceholder := len(args) + 1
	args = append(args, limit+1)

	query := buildQueryResourcesSQL(predicateSQL, keysetSQL, limitPlaceholder)
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
			Kind:           string(last.result.Kind),
			ServiceName:    string(last.result.ServiceName),
			CollectionName: string(last.result.CollectionName),
			ResourceID:     string(last.result.ResourceID),
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
// final SELECT: the envelope columns plus the full extension/platform
// projection columns (nullable, since only one side's LATERAL join
// matches any given row). It builds the matching side's read model by
// delegating to extensionResourceViewFromColumns/
// platformResourceAggregateFromColumns -- the same construction logic
// [ExtensionResourceRepo.GetView] and [ResourceIdentityRepo.GetByName]
// use -- so the projection stays provably equivalent to those reads
// (see queryrepotest's equivalence tests) without a second, per-row
// database round trip.
func scanQueryResourceRow(s scanner) (queryResourceRow, error) {
	var kind, envName, platformName string
	var resourceType, apiVersion sql.NullString
	var serviceName, collectionName, resourceID, typeName string

	// Extension projection columns (extv.*): all nullable here since
	// the LATERAL join over erViewQueryPG produces no row at all for
	// platform-kind envelope rows.
	var extUID domain.ExtensionResourceUID
	var extServiceName, extTypeName, extCollectionName, extResourceID sql.NullString
	var extLabels, extReportedAliases sql.NullString
	var extCreatedAt, extUpdatedAt *time.Time
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

	// Platform projection columns (plat.*): all nullable for
	// extension-kind envelope rows, for the same reason as above.
	var platCollectionName, platResourceID, platLabels sql.NullString
	var platCreatedAt, platUpdatedAt sql.NullString
	var platRepresentations, platAliases, platRelationships sql.NullString

	if err := s.Scan(
		&kind, &envName, &platformName, &resourceType, &serviceName, &apiVersion,
		&collectionName, &resourceID, &typeName,

		&extUID, &extServiceName, &extTypeName, &extCollectionName, &extResourceID, &extLabels, &extReportedAliases,
		&extCreatedAt, &extUpdatedAt,
		&extCurrentVersion, &extFulfillmentID,
		&riSpec, &riCreatedAt,
		&fID, &msVer, &msSpec, &psVer, &psSpec, &rsVer, &rsSpec,
		&rtJSON, &stateStr, &pauseReason, &statusReason, &authJSON, &provJSON, &attestRefJSON,
		&generation, &observedGeneration, &activeWorkflowGen,
		&fCreatedAt, &fUpdatedAt,
		&invLabels, &invObservation, &invObservedAt, &invUpdatedAt, &invConditionsJSON,

		&platCollectionName, &platResourceID, &platLabels, &platCreatedAt, &platUpdatedAt,
		&platRepresentations, &platAliases, &platRelationships,
	); err != nil {
		return queryResourceRow{}, fmt.Errorf("scan query resource row: %w", err)
	}

	result := domain.QueryResourceResult{
		Kind:           domain.QueryResourceKind(kind),
		Name:           envName,
		ServiceName:    domain.ServiceName(serviceName),
		CollectionName: domain.CollectionName(collectionName),
		ResourceID:     domain.ResourceID(resourceID),
	}
	if resourceType.Valid {
		result.ResourceType = domain.ResourceType(resourceType.String)
	}
	if apiVersion.Valid {
		result.APIVersion = domain.APIVersion(apiVersion.String)
	}

	switch result.Kind {
	case domain.QueryResourceKindExtension:
		if extCreatedAt == nil || extUpdatedAt == nil {
			return queryResourceRow{}, fmt.Errorf("query resources: extension row %q is missing its extension projection columns", envName)
		}
		view, err := extensionResourceViewFromColumns(
			extUID, extServiceName.String, extTypeName.String, extCollectionName.String, extResourceID.String,
			extLabels.String, extReportedAliases.String,
			*extCreatedAt, *extUpdatedAt,
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
			return queryResourceRow{}, fmt.Errorf("query resources: build extension view for %q: %w", envName, err)
		}
		result.Extension = &view
	case domain.QueryResourceKindPlatform:
		if !platCollectionName.Valid || !platResourceID.Valid {
			return queryResourceRow{}, fmt.Errorf("query resources: platform row %q is missing its platform projection columns", envName)
		}
		pr, err := platformResourceAggregateFromColumns(
			platCollectionName.String, platResourceID.String, platLabels.String,
			platCreatedAt.String, platUpdatedAt.String,
			platRepresentations.String, platAliases.String, platRelationships.String,
		)
		if err != nil {
			return queryResourceRow{}, fmt.Errorf("query resources: build platform resource for %q: %w", envName, err)
		}
		result.Platform = pr
	default:
		return queryResourceRow{}, fmt.Errorf("query resources: unexpected row kind %q", kind)
	}

	return queryResourceRow{result: result, typeName: typeName}, nil
}
