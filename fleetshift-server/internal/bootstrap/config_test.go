package bootstrap_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/bootstrap"
)

func TestNewConfig(t *testing.T) {
	validCA := mustTestCAPEM(t)

	tests := []struct {
		name    string
		in      bootstrap.ConfigInput
		want    bootstrap.Config
		wantErr string
	}{
		{
			name: "valid sqlite defaults",
			in: bootstrap.ConfigInput{
				GRPCAddr: ":50051",
				HTTPAddr: ":8080",
				DBPath:   bootstrap.DefaultSQLitePath,
				Addons:   "kind,kubernetes",
			},
			want: bootstrap.Config{
				GRPCAddr: ":50051",
				HTTPAddr: ":8080",
				Database: bootstrap.SQLite{Path: bootstrap.DefaultSQLitePath},
				Addons:   []bootstrap.AddonName{bootstrap.AddonKind, bootstrap.AddonKubernetes},
			},
		},
		{
			name: "valid postgres URL",
			in: bootstrap.ConfigInput{
				GRPCAddr:    ":50051",
				HTTPAddr:    ":8080",
				DBPath:      bootstrap.DefaultSQLitePath,
				DatabaseURL: "postgres://user:pass@localhost:5432/fleetshift?sslmode=disable",
			},
			want: bootstrap.Config{
				GRPCAddr: ":50051",
				HTTPAddr: ":8080",
				Database: bootstrap.Postgres{
					Host:      "localhost",
					Port:      5432,
					User:      "user",
					Password:  "pass",
					Name:      "fleetshift",
					Params:    url.Values{"sslmode": []string{"disable"}},
					DriverDSN: "postgres://user:pass@localhost:5432/fleetshift?sslmode=disable",
				},
			},
		},
		{
			name: "postgres from file content",
			in: bootstrap.ConfigInput{
				GRPCAddr:               ":50051",
				HTTPAddr:               ":8080",
				DBPath:                 bootstrap.DefaultSQLitePath,
				DatabaseURLFileSet:     true,
				DatabaseURLFileContent: "postgres://user:pass@localhost:5432/from-file",
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
			name: "valid CA bundle is copied",
			in: bootstrap.ConfigInput{
				GRPCAddr:     ":50051",
				HTTPAddr:     ":8080",
				DBPath:       bootstrap.DefaultSQLitePath,
				OIDCCABundle: validCA,
			},
			want: bootstrap.Config{
				GRPCAddr:     ":50051",
				HTTPAddr:     ":8080",
				Database:     bootstrap.SQLite{Path: bootstrap.DefaultSQLitePath},
				OIDCCABundle: validCA,
			},
		},
		{
			name: "gcphcp with config path",
			in: bootstrap.ConfigInput{
				GRPCAddr:         ":50051",
				HTTPAddr:         ":8080",
				DBPath:           bootstrap.DefaultSQLitePath,
				Addons:           "gcphcp",
				GCPHCPConfigPath: "/tmp/gcphcp.yaml",
			},
			want: bootstrap.Config{
				GRPCAddr:         ":50051",
				HTTPAddr:         ":8080",
				Database:         bootstrap.SQLite{Path: bootstrap.DefaultSQLitePath},
				Addons:           []bootstrap.AddonName{bootstrap.AddonGCPHCP},
				GCPHCPConfigPath: "/tmp/gcphcp.yaml",
			},
		},
		{
			name: "addon whitespace and duplicates normalized",
			in: bootstrap.ConfigInput{
				GRPCAddr: ":50051",
				HTTPAddr: ":8080",
				DBPath:   bootstrap.DefaultSQLitePath,
				Addons:   " kind , kubernetes ,kind ",
			},
			want: bootstrap.Config{
				GRPCAddr: ":50051",
				HTTPAddr: ":8080",
				Database: bootstrap.SQLite{Path: bootstrap.DefaultSQLitePath},
				Addons:   []bootstrap.AddonName{bootstrap.AddonKind, bootstrap.AddonKubernetes},
			},
		},
		{
			name: "non-default db path without explicit flag keeps postgres when URL set",
			in: bootstrap.ConfigInput{
				GRPCAddr:    ":50051",
				HTTPAddr:    ":8080",
				DBPath:      "custom.db",
				DBExplicit:  false,
				DatabaseURL: "postgres://user:pass@localhost:5432/db",
			},
			want: bootstrap.Config{
				GRPCAddr: ":50051",
				HTTPAddr: ":8080",
				Database: bootstrap.Postgres{
					Host:      "localhost",
					Port:      5432,
					User:      "user",
					Password:  "pass",
					Name:      "db",
					Params:    url.Values{},
					DriverDSN: "postgres://user:pass@localhost:5432/db",
				},
			},
		},
		{
			name: "explicit db with default path still conflicts with URL",
			in: bootstrap.ConfigInput{
				GRPCAddr:    ":50051",
				HTTPAddr:    ":8080",
				DBPath:      bootstrap.DefaultSQLitePath,
				DBExplicit:  true,
				DatabaseURL: "postgres://user:pass@localhost:5432/db",
			},
			wantErr: "--database-url and --db are mutually exclusive",
		},
		{
			name: "empty database URL file does not fall back to SQLite",
			in: bootstrap.ConfigInput{
				GRPCAddr:               ":50051",
				HTTPAddr:               ":8080",
				DBPath:                 bootstrap.DefaultSQLitePath,
				DatabaseURLFileSet:     true,
				DatabaseURLFileContent: "",
			},
			wantErr: "--database-url-file is set but contains no database URL",
		},
		{
			name: "whitespace-only database URL file does not fall back to SQLite",
			in: bootstrap.ConfigInput{
				GRPCAddr:               ":50051",
				HTTPAddr:               ":8080",
				DBPath:                 bootstrap.DefaultSQLitePath,
				DatabaseURLFileSet:     true,
				DatabaseURLFileContent: "  \n\t",
			},
			wantErr: "--database-url-file is set but contains no database URL",
		},
		{
			name: "file and database-url conflict",
			in: bootstrap.ConfigInput{
				GRPCAddr:               ":50051",
				HTTPAddr:               ":8080",
				DBPath:                 bootstrap.DefaultSQLitePath,
				DatabaseURL:            "postgres://inline",
				DatabaseURLFileSet:     true,
				DatabaseURLFileContent: "postgres://from-file",
			},
			wantErr: "--database-url-file and --database-url are mutually exclusive",
		},
		{
			name: "file and explicit db conflict",
			in: bootstrap.ConfigInput{
				GRPCAddr:               ":50051",
				HTTPAddr:               ":8080",
				DBPath:                 "custom.db",
				DBExplicit:             true,
				DatabaseURLFileSet:     true,
				DatabaseURLFileContent: "postgres://from-file",
			},
			wantErr: "--database-url-file and --db are mutually exclusive",
		},
		{
			name: "missing grpc addr",
			in: bootstrap.ConfigInput{
				HTTPAddr: ":8080",
				DBPath:   bootstrap.DefaultSQLitePath,
			},
			wantErr: "grpc listen address is required",
		},
		{
			name: "missing http addr",
			in: bootstrap.ConfigInput{
				GRPCAddr: ":50051",
				DBPath:   bootstrap.DefaultSQLitePath,
			},
			wantErr: "http listen address is required",
		},
		{
			name: "neither database mode",
			in: bootstrap.ConfigInput{
				GRPCAddr: ":50051",
				HTTPAddr: ":8080",
			},
			wantErr: "exactly one of SQLite path or database URL is required",
		},
		{
			name: "invalid database URL scheme",
			in: bootstrap.ConfigInput{
				GRPCAddr:    ":50051",
				HTTPAddr:    ":8080",
				DatabaseURL: "mysql://user:pass@localhost:3306/db",
			},
			wantErr: "scheme must be postgres or postgresql",
		},
		{
			name: "database URL missing host",
			in: bootstrap.ConfigInput{
				GRPCAddr:    ":50051",
				HTTPAddr:    ":8080",
				DatabaseURL: "postgres:///dbname",
			},
			wantErr: "host is required",
		},
		{
			name: "database URL missing db name",
			in: bootstrap.ConfigInput{
				GRPCAddr:    ":50051",
				HTTPAddr:    ":8080",
				DatabaseURL: "postgres://user:pass@localhost:5432",
			},
			wantErr: "database name is required",
		},
		{
			name: "unknown addon",
			in: bootstrap.ConfigInput{
				GRPCAddr: ":50051",
				HTTPAddr: ":8080",
				DBPath:   bootstrap.DefaultSQLitePath,
				Addons:   "not-an-addon",
			},
			wantErr: `unknown addon "not-an-addon"`,
		},
		{
			name: "gcphcp without config path",
			in: bootstrap.ConfigInput{
				GRPCAddr: ":50051",
				HTTPAddr: ":8080",
				DBPath:   bootstrap.DefaultSQLitePath,
				Addons:   "gcphcp",
			},
			wantErr: "GCPHCP_CONFIG",
		},
		{
			name: "invalid OIDC UI authority",
			in: bootstrap.ConfigInput{
				GRPCAddr:        ":50051",
				HTTPAddr:        ":8080",
				DBPath:          bootstrap.DefaultSQLitePath,
				OIDCUIAuthority: "://bad",
			},
			wantErr: "oidc UI authority",
		},
		{
			name: "OIDC UI authority missing host",
			in: bootstrap.ConfigInput{
				GRPCAddr:        ":50051",
				HTTPAddr:        ":8080",
				DBPath:          bootstrap.DefaultSQLitePath,
				OIDCUIAuthority: "http:///path",
			},
			wantErr: "host is required",
		},
		{
			name: "invalid CA data",
			in: bootstrap.ConfigInput{
				GRPCAddr:     ":50051",
				HTTPAddr:     ":8080",
				DBPath:       bootstrap.DefaultSQLitePath,
				OIDCCABundle: []byte("not-pem"),
			},
			wantErr: "invalid OIDC CA data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := bootstrap.NewConfig(tt.in)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("NewConfig() = nil error, want %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("NewConfig() error = %q, want substring %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewConfig() unexpected error: %v", err)
			}
			assertConfigEqual(t, got, tt.want)
			if len(tt.in.OIDCCABundle) > 0 && len(got.OIDCCABundle) > 0 && &got.OIDCCABundle[0] == &tt.in.OIDCCABundle[0] {
				t.Fatal("OIDCCABundle was not copied")
			}
		})
	}
}

func TestConfigAddonSet(t *testing.T) {
	cfg, err := bootstrap.NewConfig(bootstrap.ConfigInput{
		GRPCAddr:         ":50051",
		HTTPAddr:         ":8080",
		DBPath:           bootstrap.DefaultSQLitePath,
		Addons:           "kind,gcphcp",
		GCPHCPConfigPath: "/tmp/gcphcp.yaml",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.AddonSet()
	if !got[bootstrap.AddonKind] || !got[bootstrap.AddonGCPHCP] || got[bootstrap.AddonKubernetes] {
		t.Fatalf("AddonSet() = %#v", got)
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
	assertDatabaseEqual(t, got.Database, want.Database)
}

func assertDatabaseEqual(t *testing.T, got, want bootstrap.Database) {
	t.Helper()
	switch wantDB := want.(type) {
	case bootstrap.SQLite:
		gotDB, ok := got.(bootstrap.SQLite)
		if !ok {
			t.Fatalf("Database type = %T, want SQLite", got)
		}
		if gotDB != wantDB {
			t.Fatalf("SQLite = %#v, want %#v", gotDB, wantDB)
		}
	case bootstrap.Postgres:
		gotDB, ok := got.(bootstrap.Postgres)
		if !ok {
			t.Fatalf("Database type = %T, want Postgres", got)
		}
		if gotDB.Host != wantDB.Host ||
			gotDB.Port != wantDB.Port ||
			gotDB.User != wantDB.User ||
			gotDB.Password != wantDB.Password ||
			gotDB.Name != wantDB.Name ||
			gotDB.DriverDSN != wantDB.DriverDSN ||
			gotDB.Params.Encode() != wantDB.Params.Encode() {
			t.Fatalf("Postgres = %#v, want %#v", gotDB, wantDB)
		}
	default:
		t.Fatalf("unexpected want database type %T", want)
	}
}

func mustTestCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "fleetshift-test-ca"},
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
