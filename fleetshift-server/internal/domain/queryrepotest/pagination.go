package queryrepotest

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func runPaginationTests(t *testing.T, factory Factory) {
	t.Run("PageSizeLimitsResultsAndReturnsNextPageToken", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		page, err := tx.Queries().QueryResources(context.Background(), domain.QueryResourcesRequest{PageSize: 2})
		if err != nil {
			t.Fatalf("QueryResources: %v", err)
		}
		if len(page.Resources) != 2 {
			t.Fatalf("len(Resources) = %d, want 2", len(page.Resources))
		}
		if page.NextPageToken == "" {
			t.Fatalf("NextPageToken is empty, want non-empty (5 fixture rows > page size 2)")
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
				PageSize:  2,
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
		if len(seen) != 5 {
			t.Errorf("total unique results across pages = %d, want 5", len(seen))
		}
	})

	t.Run("PageTokenWithDifferentFilterIsInvalid", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		page, err := tx.Queries().QueryResources(ctx, domain.QueryResourcesRequest{
			Filter:   `kind == "platform"`,
			PageSize: 1,
		})
		if err != nil {
			t.Fatalf("QueryResources: %v", err)
		}
		if page.NextPageToken == "" {
			t.Fatalf("NextPageToken is empty, want non-empty (need a token to replay against a different filter)")
		}

		_, err = tx.Queries().QueryResources(ctx, domain.QueryResourcesRequest{
			Filter:    `kind == "extension"`,
			PageSize:  1,
			PageToken: page.NextPageToken,
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("NonEmptyOrderByIsUnimplemented", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		err := queryErr(t, tx, domain.QueryResourcesRequest{OrderBy: "name"})
		if !errors.Is(err, domain.ErrUnimplemented) {
			t.Errorf("err = %v, want ErrUnimplemented", err)
		}
	})
}
