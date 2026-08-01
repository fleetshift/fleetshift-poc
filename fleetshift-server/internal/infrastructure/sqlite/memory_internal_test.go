package sqlite

import "testing"

func TestOpenSharedMemoryDSN_RejectsNonSharedDSN(t *testing.T) {
	for _, dsn := range []string{":memory:", "/tmp/fleetshift.db", "file:x?mode=memory"} {
		if _, _, err := openSharedMemoryDSN(dsn); err == nil {
			t.Fatalf("openSharedMemoryDSN(%q) succeeded, want error", dsn)
		}
	}
	if !isSharedMemoryDSN(sharedMemoryDSN("x")) {
		t.Fatal("sharedMemoryDSN should satisfy isSharedMemoryDSN")
	}
}
