package cli

import (
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func TestValidateConfigFlags(t *testing.T) {
	tests := []struct {
		name  string
		flags globalFlags
		want  string
	}{
		{
			name:  "defaults",
			flags: globalFlags{},
		},
		{
			name:  "config dir without insecure storage",
			flags: globalFlags{configDir: "/tmp/fleetctl-config"},
		},
		{
			name:  "insecure storage requires config dir",
			flags: globalFlags{insecureStorage: true},
			want:  "--insecure-storage requires --config-dir",
		},
		{
			name:  "insecure storage with relative config dir",
			flags: globalFlags{insecureStorage: true, configDir: "relative"},
			want:  "--config-dir must be an absolute path",
		},
		{
			name:  "relative config dir",
			flags: globalFlags{configDir: "relative"},
			want:  "--config-dir must be an absolute path",
		},
		{
			name:  "insecure storage with absolute config dir",
			flags: globalFlags{insecureStorage: true, configDir: "/tmp/fleetctl-config"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConfigFlags(tt.flags)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("validateConfigFlags() unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateConfigFlags() expected error containing %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tt.want)
			}
		})
	}
}

func TestStore_DefaultIsKeyring(t *testing.T) {
	store := (globalFlags{}).store()
	if _, ok := store.(auth.KeyringStore); !ok {
		t.Fatalf("got %T, want KeyringStore", store)
	}
}

func TestStore_InsecureStorageUsesFileStore(t *testing.T) {
	const dir = "/tmp/fleetctl-config"
	store := (globalFlags{configDir: dir, insecureStorage: true}).store()
	fs, ok := store.(auth.FileStore)
	if !ok {
		t.Fatalf("got %T, want FileStore", store)
	}
	if fs.Dir != dir {
		t.Fatalf("Dir = %q, want %q", fs.Dir, dir)
	}
}

func TestStore_ConfigDirWithoutInsecureStorageIsKeyring(t *testing.T) {
	store := (globalFlags{configDir: "/tmp/fleetctl-config"}).store()
	if _, ok := store.(auth.KeyringStore); !ok {
		t.Fatalf("got %T, want KeyringStore", store)
	}
}

func TestConfigDirAndInsecureStorageFlags(t *testing.T) {
	cmd := New()
	if cmd.PersistentFlags().Lookup("insecure-storage") == nil {
		t.Fatal("missing --insecure-storage")
	}
	if cmd.PersistentFlags().Lookup("config-dir") == nil {
		t.Fatal("missing --config-dir")
	}
	if cmd.PersistentFlags().Lookup("state-dir") != nil {
		t.Fatal("--state-dir was renamed to --config-dir")
	}
}
