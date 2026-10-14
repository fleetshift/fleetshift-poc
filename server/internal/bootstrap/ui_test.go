package bootstrap_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/bootstrap"
)

func TestDeriveUIOrigin(t *testing.T) {
	tests := []struct {
		addr    string
		want    string
		wantErr bool
	}{
		{addr: ":8085", want: "http://127.0.0.1:8085"},
		{addr: "0.0.0.0:8085", want: "http://127.0.0.1:8085"},
		{addr: "127.0.0.1:8085", want: "http://127.0.0.1:8085"},
		{addr: "[::]:8085", want: "http://127.0.0.1:8085"},
		{addr: "bad", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			got, err := bootstrap.DeriveUIOrigin(tt.addr)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("DeriveUIOrigin(%q) = %q, want %q", tt.addr, got, tt.want)
			}
		})
	}
}
