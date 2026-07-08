package queryrepotest

import (
	"reflect"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// assertExtensionViewEqual compares got against want by snapshot,
// rather than by reference/pointer identity, since got and want come
// from two independent scans (the QueryRepository projection and a
// direct [domain.ExtensionResourceRepository.GetView] call). Both
// sides are read-path constructions built by the same underlying
// column-to-domain-object logic (see
// ../../infrastructure/postgres/extension_resource_repo.go's
// extensionResourceViewFromColumns), so their [domain.Condition]/
// representation/etc. slice orderings are expected to already match
// without any sorting here.
func assertExtensionViewEqual(t *testing.T, got, want domain.ExtensionResourceView) {
	t.Helper()

	gotSnap := got.Resource.Snapshot()
	wantSnap := want.Resource.Snapshot()
	if !reflect.DeepEqual(gotSnap, wantSnap) {
		t.Errorf("Extension.Resource.Snapshot() mismatch:\n got:  %+v\n want: %+v", gotSnap, wantSnap)
	}

	if !reflect.DeepEqual(got.Intent, want.Intent) {
		t.Errorf("Extension.Intent mismatch:\n got:  %+v\n want: %+v", got.Intent, want.Intent)
	}

	switch {
	case got.Fulfillment == nil && want.Fulfillment == nil:
		// both unmanaged; nothing further to compare.
	case got.Fulfillment == nil || want.Fulfillment == nil:
		t.Errorf("Extension.Fulfillment presence mismatch: got %v, want %v", got.Fulfillment, want.Fulfillment)
	default:
		gotFSnap := got.Fulfillment.Snapshot()
		wantFSnap := want.Fulfillment.Snapshot()
		if !reflect.DeepEqual(gotFSnap, wantFSnap) {
			t.Errorf("Extension.Fulfillment.Snapshot() mismatch:\n got:  %+v\n want: %+v", gotFSnap, wantFSnap)
		}
	}
}

// assertPlatformResourceEqual compares got against want by snapshot;
// see assertExtensionViewEqual's doc for why this is safe against the
// two independent read paths being compared.
func assertPlatformResourceEqual(t *testing.T, got, want *domain.PlatformResource) {
	t.Helper()
	if got == nil || want == nil {
		t.Fatalf("assertPlatformResourceEqual: got=%v want=%v, neither may be nil", got, want)
	}
	gotSnap := got.Snapshot()
	wantSnap := want.Snapshot()
	if !reflect.DeepEqual(gotSnap, wantSnap) {
		t.Errorf("Platform.Snapshot() mismatch:\n got:  %+v\n want: %+v", gotSnap, wantSnap)
	}
}
