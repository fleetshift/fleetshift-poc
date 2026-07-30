package cli

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/serverapp"
)

func TestLoadServeConfig(t *testing.T) {
	validCA := mustCLITestCAPEM(t)
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
		want    serverapp.Config
		wantErr string
	}{
		{
			name: "default sqlite",
			flags: serveFlags{
				grpcAddr: ":50051",
				httpAddr: ":8080",
				dbPath:   serverapp.DefaultSQLitePath,
				addons:   "kind,kubernetes",
			},
			want: serverapp.Config{
				GRPCAddr: ":50051",
				HTTPAddr: ":8080",
				Database: serverapp.SQLite{Path: serverapp.DefaultSQLitePath},
				Addons:   []serverapp.AddonName{serverapp.AddonKind, serverapp.AddonKubernetes},
			},
		},
		{
			name: "reads database URL file without mutating flags",
			flags: serveFlags{
				grpcAddr:        ":50051",
				httpAddr:        ":8080",
				dbPath:          serverapp.DefaultSQLitePath,
				databaseURLFile: urlPath,
			},
			want: serverapp.Config{
				GRPCAddr: ":50051",
				HTTPAddr: ":8080",
				Database: serverapp.Postgres{
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
				dbPath:     serverapp.DefaultSQLitePath,
				oidcCAFile: caPath,
			},
			want: serverapp.Config{
				GRPCAddr:     ":50051",
				HTTPAddr:     ":8080",
				Database:     serverapp.SQLite{Path: serverapp.DefaultSQLitePath},
				OIDCCABundle: validCA,
			},
		},
		{
			name: "resolves GCPHCP config from env",
			flags: serveFlags{
				grpcAddr: ":50051",
				httpAddr: ":8080",
				dbPath:   serverapp.DefaultSQLitePath,
				addons:   "gcphcp",
			},
			envGCP: "/env/gcphcp.yaml",
			want: serverapp.Config{
				GRPCAddr:         ":50051",
				HTTPAddr:         ":8080",
				Database:         serverapp.SQLite{Path: serverapp.DefaultSQLitePath},
				Addons:           []serverapp.AddonName{serverapp.AddonGCPHCP},
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
				dbPath:      serverapp.DefaultSQLitePath,
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
				dbPath:   serverapp.DefaultSQLitePath,
				addons:   "gcphcp",
			},
			wantErr: "GCPHCP_CONFIG",
		},
		{
			name: "explicit db conflicts with URL",
			flags: serveFlags{
				grpcAddr:    ":50051",
				httpAddr:    ":8080",
				dbPath:      serverapp.DefaultSQLitePath,
				databaseURL: "postgres://user:pass@localhost:5432/db",
			},
			sel:     serveSelections{DB: true},
			wantErr: "--database-url and --db are mutually exclusive",
		},
		{
			name: "file not found",
			flags: serveFlags{
				grpcAddr:        ":50051",
				httpAddr:        ":8080",
				dbPath:          serverapp.DefaultSQLitePath,
				databaseURLFile: filepath.Join(t.TempDir(), "missing-url"),
			},
			wantErr: "read database URL file",
		},
		{
			name: "empty database URL file does not fall back to SQLite",
			flags: serveFlags{
				grpcAddr:        ":50051",
				httpAddr:        ":8080",
				dbPath:          serverapp.DefaultSQLitePath,
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

func assertConfigEqual(t *testing.T, got, want serverapp.Config) {
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
	case serverapp.SQLite:
		gotDB, ok := got.Database.(serverapp.SQLite)
		if !ok || gotDB != wantDB {
			t.Fatalf("Database = %#v, want %#v", got.Database, want.Database)
		}
	case serverapp.Postgres:
		gotDB, ok := got.Database.(serverapp.Postgres)
		if !ok ||
			gotDB.Host != wantDB.Host ||
			gotDB.Port != wantDB.Port ||
			gotDB.User != wantDB.User ||
			gotDB.Password != wantDB.Password ||
			gotDB.Name != wantDB.Name ||
			gotDB.DriverDSN != wantDB.DriverDSN ||
			gotDB.Params.Encode() != wantDB.Params.Encode() {
			t.Fatalf("Database = %#v, want %#v", got.Database, want.Database)
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

func mustCLITestCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "fleetshift-cli-test-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
