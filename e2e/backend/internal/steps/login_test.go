package steps

import (
	"strings"
	"testing"
)

func TestTokenEmail(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		in     string
		want   string
		errSub string
	}{
		{
			name: "access token",
			in:   `{"access_token":{"claims":{"email":"dev@fleetshift.local"}}}`,
			want: "dev@fleetshift.local",
		},
		{
			name: "id token fallback",
			in:   `{"id_token":{"claims":{"email":"ops@fleetshift.local"}}}`,
			want: "ops@fleetshift.local",
		},
		{
			name: "blank access then id token",
			in:   `{"access_token":{"claims":{"email":"  "}},"id_token":{"claims":{"email":"ops@fleetshift.local"}}}`,
			want: "ops@fleetshift.local",
		},
		{
			name: "non-string email skipped",
			in:   `{"access_token":{"claims":{"email":1}},"id_token":{"claims":{"email":"dev@fleetshift.local"}}}`,
			want: "dev@fleetshift.local",
		},
		{
			name:   "missing",
			in:     `{"token_type":"Bearer"}`,
			errSub: "email",
		},
		{
			name:   "blank only",
			in:     `{"access_token":{"claims":{"email":"  "}}}`,
			errSub: "email",
		},
		{
			name:   "invalid",
			in:     `{`,
			errSub: "parse inspect-token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tokenEmail(tt.in)
			if tt.errSub != "" {
				if err == nil {
					t.Fatal("expected error")
				}
				if !strings.Contains(err.Error(), tt.errSub) {
					t.Fatalf("error = %v, want %q", err, tt.errSub)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}
