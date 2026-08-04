package cli

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/bootstrap"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/testutil"
)

func TestLoadServeConfig(t *testing.T) {
	validCA := testutil.MustCAPEM(t)
	caPath := filepath.Join(t.TempDir(), "oidc-ca.pem")
	if err := os.WriteFile(caPath, validCA, 0600); err != nil {
		t.Fatal(err)
	}

	urlPath := filepath.Join(t.TempDir(), "db-url")
	emptyURLPath := filepath.Join(t.TempDir(), "empty-db-url")
	if err := os.WriteFile(urlPath, []byte("postgres://user:pass@localhost:5432/from-file\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(emptyURLPath, []byte("  \n\t"), 0600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		flags   serveFlags
		sel     serveSelections
		envGCP  string
		want    bootstrap.Config
		wantErr string
	}{
		{
			name: "default sqlite",
			flags: serveFlags{
				grpcAddr: ":50051",
				httpAddr: ":8080",
				dbPath:   bootstrap.DefaultSQLitePath,
				addons:   "kind,kubernetes",
			},
			want: bootstrap.Config{
				GRPCAddr: ":50051",
				HTTPAddr: ":8080",
				Database: bootstrap.SQLite{Path: bootstrap.DefaultSQLitePath},
				Addons:   []bootstrap.AddonName{bootstrap.AddonKind, bootstrap.AddonKubernetes},
			},
		},
		{
			name: "reads database URL file without mutating flags",
			flags: serveFlags{
				grpcAddr:        ":50051",
				httpAddr:        ":8080",
				dbPath:          bootstrap.DefaultSQLitePath,
				databaseURLFile: urlPath,
			},
			want: bootstrap.Config{
				GRPCAddr: ":50051",
				HTTPAddr: ":8080",
				Database: bootstrap.Postgres{
					Host:      "localhost",
					Port:      5432,
					User:      "user",
					Password:  "pass",
					Name:      "from-file",
					Params:    url.Values{},
					DriverDSN: "postgres://user:pass@localhost:5432/from-file",
				},
			},
		},
		{
			name: "reads OIDC CA file",
			flags: serveFlags{
				grpcAddr:   ":50051",
				httpAddr:   ":8080",
				dbPath:     bootstrap.DefaultSQLitePath,
				oidcCAFile: caPath,
			},
			want: bootstrap.Config{
				GRPCAddr:     ":50051",
				HTTPAddr:     ":8080",
				Database:     bootstrap.SQLite{Path: bootstrap.DefaultSQLitePath},
				OIDCCABundle: validCA,
			},
		},
		{
			name: "resolves GCPHCP config from env",
			flags: serveFlags{
				grpcAddr: ":50051",
				httpAddr: ":8080",
				dbPath:   bootstrap.DefaultSQLitePath,
				addons:   "gcphcp",
			},
			envGCP: "/env/gcphcp.yaml",
			want: bootstrap.Config{
				GRPCAddr:         ":50051",
				HTTPAddr:         ":8080",
				Database:         bootstrap.SQLite{Path: bootstrap.DefaultSQLitePath},
				Addons:           []bootstrap.AddonName{bootstrap.AddonGCPHCP},
				GCPHCPConfigPath: "/env/gcphcp.yaml",
			},
		},
		{
			name: "unknown addon rejected before resource I/O",
			flags: serveFlags{
				grpcAddr: ":50051",
				httpAddr: ":8080",
				dbPath:   "/nonexistent/dir/should-not-open.db",
				addons:   "not-a-real-addon",
			},
			wantErr: `unknown addon "not-a-real-addon"`,
		},
		{
			name: "invalid database URL rejected before resource I/O",
			flags: serveFlags{
				grpcAddr:    ":50051",
				httpAddr:    ":8080",
				dbPath:      bootstrap.DefaultSQLitePath,
				databaseURL: "mysql://localhost/db",
			},
			wantErr: "scheme must be postgres or postgresql",
		},
		{
			name: "invalid CA rejected before resource I/O",
			flags: serveFlags{
				grpcAddr:   ":50051",
				httpAddr:   ":8080",
				dbPath:     "/nonexistent/dir/should-not-open.db",
				oidcCAFile: writeTempFile(t, "bad.pem", []byte("not-pem")),
			},
			wantErr: "invalid OIDC CA data",
		},
		{
			name: "gcphcp without config rejected",
			flags: serveFlags{
				grpcAddr: ":50051",
				httpAddr: ":8080",
				dbPath:   bootstrap.DefaultSQLitePath,
				addons:   "gcphcp",
			},
			wantErr: "GCPHCP_CONFIG",
		},
		{
			name: "explicit db conflicts with URL",
			flags: serveFlags{
				grpcAddr:    ":50051",
				httpAddr:    ":8080",
				dbPath:      bootstrap.DefaultSQLitePath,
				databaseURL: "postgres://user:pass@localhost:5432/db",
			},
			sel:     serveSelections{DBExplicit: true},
			wantErr: "--database-url and --db are mutually exclusive",
		},
		{
			name: "file not found",
			flags: serveFlags{
				grpcAddr:        ":50051",
				httpAddr:        ":8080",
				dbPath:          bootstrap.DefaultSQLitePath,
				databaseURLFile: filepath.Join(t.TempDir(), "missing-url"),
			},
			wantErr: "read database URL file",
		},
		{
			name: "empty database URL file does not fall back to SQLite",
			flags: serveFlags{
				grpcAddr:        ":50051",
				httpAddr:        ":8080",
				dbPath:          bootstrap.DefaultSQLitePath,
				databaseURLFile: emptyURLPath,
			},
			wantErr: "--database-url-file is set but contains no database URL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("GCPHCP_CONFIG", tt.envGCP)
			flags := tt.flags
			originalURL := flags.databaseURL
			originalDB := flags.dbPath
			originalFile := flags.databaseURLFile

			got, err := loadServeConfig(&flags, tt.sel)
			if flags.databaseURL != originalURL || flags.dbPath != originalDB || flags.databaseURLFile != originalFile {
				t.Fatalf("loadServeConfig mutated serveFlags: url=%q db=%q file=%q", flags.databaseURL, flags.dbPath, flags.databaseURLFile)
			}
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("loadServeConfig() = nil error, want %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("loadServeConfig() error = %q, want substring %q", err.Error(), tt.wantErr)
				}
				if strings.Contains(err.Error(), "open database") {
					t.Fatalf("rejection performed database I/O: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadServeConfig() unexpected error: %v", err)
			}
			assertConfigEqual(t, got, tt.want)
		})
	}
}

func TestReadDatabaseURLFile(t *testing.T) {
	tests := []struct {
		name        string
		fileContent string
		missing     bool
		wantURL     string
		wantErr     string
	}{
		{
			name:        "reads URL from file",
			fileContent: "postgres://user:pass@host:5432/db?sslmode=disable\n",
			wantURL:     "postgres://user:pass@host:5432/db?sslmode=disable",
		},
		{
			name:        "trims whitespace",
			fileContent: "  postgres://user:pass@host:5432/db  \n",
			wantURL:     "postgres://user:pass@host:5432/db",
		},
		{
			name:    "file not found",
			missing: true,
			wantErr: "read database URL file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "url-file")
			if tt.missing {
				path = filepath.Join(t.TempDir(), "nonexistent")
			} else if err := os.WriteFile(path, []byte(tt.fileContent), 0400); err != nil {
				t.Fatal(err)
			}

			got, err := readDatabaseURLFile(path)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantURL {
				t.Errorf("readDatabaseURLFile() = %q, want %q", got, tt.wantURL)
			}
		})
	}
}

func assertConfigEqual(t *testing.T, got, want bootstrap.Config) {
	t.Helper()
	if got.GRPCAddr != want.GRPCAddr ||
		got.HTTPAddr != want.HTTPAddr ||
		got.WebDir != want.WebDir ||
		got.OIDCUIAuthority != want.OIDCUIAuthority ||
		got.OIDCUIClientID != want.OIDCUIClientID ||
		got.GCPHCPConfigPath != want.GCPHCPConfigPath ||
		string(got.OIDCCABundle) != string(want.OIDCCABundle) {
		t.Fatalf("config mismatch:\n got: %#v\nwant: %#v", got, want)
	}
	if len(got.Addons) != len(want.Addons) {
		t.Fatalf("Addons = %#v, want %#v", got.Addons, want.Addons)
	}
	for i := range want.Addons {
		if got.Addons[i] != want.Addons[i] {
			t.Fatalf("Addons = %#v, want %#v", got.Addons, want.Addons)
		}
	}
	switch wantDB := want.Database.(type) {
	case bootstrap.SQLite:
		gotDB, ok := got.Database.(bootstrap.SQLite)
		if !ok || gotDB != wantDB {
			t.Fatalf("Database = %#v, want %#v", got.Database, want.Database)
		}
	case bootstrap.Postgres:
		gotDB, ok := got.Database.(bootstrap.Postgres)
		if !ok ||
			gotDB.Host != wantDB.Host ||
			gotDB.Port != wantDB.Port ||
			gotDB.User != wantDB.User ||
			gotDB.Password != wantDB.Password ||
			gotDB.Name != wantDB.Name ||
			gotDB.DriverDSN != wantDB.DriverDSN ||
			gotDB.Params.Encode() != wantDB.Params.Encode() {
			t.Fatalf("Database = %v (password_match=%t dsn_match=%t), want %v",
				gotDB, gotDB.Password == wantDB.Password, gotDB.DriverDSN == wantDB.DriverDSN, wantDB)
		}
	default:
		t.Fatalf("unexpected want database type %T", want.Database)
	}
}

func writeTempFile(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
