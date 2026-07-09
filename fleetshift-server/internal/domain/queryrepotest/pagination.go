package queryrepotest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func runPaginationTests(t *testing.T, factory Factory) {
	t.Run("PageSizeLimitsResultsAndReturnsNextPageToken", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		page, err := tx.Queries().QueryResources(context.Background(), domain.QueryResourcesRequest{PageSize: 1})
		if err != nil {
			t.Fatalf("QueryResources: %v", err)
		}
		if len(page.Resources) != 1 {
			t.Fatalf("len(Resources) = %d, want 1", len(page.Resources))
		}
		if page.NextPageToken == "" {
			t.Fatalf("NextPageToken is empty, want non-empty (2 fixture rows > page size 1)")
		}
	})

	t.Run("DefaultOrderGroupsByCollectionThenResourceID", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, "")
		if len(results) != 2 {
			t.Fatalf("len(results) = %d, want 2", len(results))
		}
		// clusters/... sorts before nodes/... under
		// (collection_name, resource_id, service_name, type_name).
		wantFirst := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		wantSecond := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		if results[0].Name != wantFirst {
			t.Errorf("results[0].Name = %q, want %q", results[0].Name, wantFirst)
		}
		if results[1].Name != wantSecond {
			t.Errorf("results[1].Name = %q, want %q", results[1].Name, wantSecond)
		}
	})

	t.Run("SecondPageResumesWithoutDuplicates", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		seen := map[string]bool{}
		var pageToken string
		pages := 0
		for {
			pages++
			if pages > 10 {
				t.Fatalf("did not terminate after 10 pages; NextPageToken looping?")
			}
			page, err := tx.Queries().QueryResources(ctx, domain.QueryResourcesRequest{
				PageSize:  1,
				PageToken: pageToken,
			})
			if err != nil {
				t.Fatalf("QueryResources (page %d): %v", pages, err)
			}
			for _, r := range page.Resources {
				if seen[r.Name] {
					t.Errorf("duplicate result %q across pages", r.Name)
				}
				seen[r.Name] = true
			}
			if page.NextPageToken == "" {
				break
			}
			pageToken = page.NextPageToken
		}
		if len(seen) != 2 {
			t.Errorf("total unique results across pages = %d, want 2", len(seen))
		}
	})

	t.Run("PageTokenWithDifferentFilterIsInvalid", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		page, err := tx.Queries().QueryResources(ctx, domain.QueryResourcesRequest{
			Filter:   fmt.Sprintf("resource_type == %q", string(fx.ManagedType)),
			PageSize: 1,
		})
		if err != nil {
			t.Fatalf("QueryResources: %v", err)
		}
		// Only one managed row matches, so there may be no next token.
		// Mint a token against the empty filter instead when needed.
		if page.NextPageToken == "" {
			page, err = tx.Queries().QueryResources(ctx, domain.QueryResourcesRequest{PageSize: 1})
			if err != nil {
				t.Fatalf("QueryResources (empty): %v", err)
			}
			if page.NextPageToken == "" {
				t.Fatalf("NextPageToken is empty, want non-empty")
			}
			_, err = tx.Queries().QueryResources(ctx, domain.QueryResourcesRequest{
				Filter:    fmt.Sprintf("resource_type == %q", string(fx.ManagedType)),
				PageSize:  1,
				PageToken: page.NextPageToken,
			})
		} else {
			_, err = tx.Queries().QueryResources(ctx, domain.QueryResourcesRequest{
				Filter:    fmt.Sprintf("resource_type == %q", string(fx.InventoryType)),
				PageSize:  1,
				PageToken: page.NextPageToken,
			})
		}
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("PageTokenWithDifferentOrderModeIsInvalid", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		page, err := tx.Queries().QueryResources(ctx, domain.QueryResourcesRequest{PageSize: 1})
		if err != nil {
			t.Fatalf("QueryResources: %v", err)
		}
		if page.NextPageToken == "" {
			t.Fatalf("NextPageToken is empty, want non-empty")
		}

		_, err = tx.Queries().QueryResources(ctx, domain.QueryResourcesRequest{
			OrderBy:   "resource_type,name",
			PageSize:  1,
			PageToken: page.NextPageToken,
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("ResourceTypeNameOrderMode", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		page, err := tx.Queries().QueryResources(context.Background(), domain.QueryResourcesRequest{
			OrderBy:  "resource_type,name",
			PageSize: 500,
		})
		if err != nil {
			t.Fatalf("QueryResources: %v", err)
		}
		if len(page.Resources) != 2 {
			t.Fatalf("len(Resources) = %d, want 2", len(page.Resources))
		}
		// kind.fleetshift.io/Cluster sorts before
		// kubernetes.fleetshift.io/Node under (service_name, type_name, ...).
		wantFirst := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		wantSecond := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		if page.Resources[0].Name != wantFirst {
			t.Errorf("Resources[0].Name = %q, want %q", page.Resources[0].Name, wantFirst)
		}
		if page.Resources[1].Name != wantSecond {
			t.Errorf("Resources[1].Name = %q, want %q", page.Resources[1].Name, wantSecond)
		}
	})

	t.Run("UnsupportedOrderByIsInvalid", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		err := queryErr(t, tx, domain.QueryResourcesRequest{OrderBy: "name"})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("PriorVersionPageTokenIsInvalid", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		// Version 1 was the previous POC token shape; current version
		// is 2. A prior-version token must fail closed.
		legacy, err := json.Marshal(map[string]any{
			"version":         1,
			"filter_hash":     "unused",
			"order_by":        "",
			"kind":            "extension",
			"service_name":    "kind.fleetshift.io",
			"collection_name": "clusters",
			"resource_id":     "managed",
			"type_name":       "Cluster",
		})
		if err != nil {
			t.Fatalf("marshal legacy token: %v", err)
		}
		tok := base64.RawURLEncoding.EncodeToString(legacy)
		err = queryErr(t, tx, domain.QueryResourcesRequest{PageToken: tok, PageSize: 1})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
	})
}
