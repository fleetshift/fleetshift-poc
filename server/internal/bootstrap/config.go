// Package bootstrap is the sole composition edge for FleetShift.
// It owns normalized production configuration, eager construction of the
// complete application graph, listener authority, semantic readiness,
// background supervision, and bounded shutdown. fleetshift serve and the
// frozen testserver facade both use Start; packages below this edge must
// not import bootstrap.
package bootstrap

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DefaultSQLitePath is the serve --db default when PostgreSQL is not selected.
const DefaultSQLitePath = "fleetshift.db"

// DefaultInitialAuthMethodTimeout bounds implicit initial-AuthMethod discovery/install
// when the AuthMethod store is empty and OIDC initial-AuthMethod config is present.
const DefaultInitialAuthMethodTimeout = 20 * time.Second

// AddonName is an addon identifier in production configuration. Allow-listed
// values are the Addon* constants; unknown names are rejected by NewConfig.
type AddonName string

// Allow-listed addon names accepted by production configuration.
const (
	AddonKind       AddonName = "kind"
	AddonKubernetes AddonName = "kubernetes"
	AddonGCPHCP     AddonName = "gcphcp"
)

var knownAddons = map[AddonName]struct{}{
	AddonKind:       {},
	AddonKubernetes: {},
	AddonGCPHCP:     {},
}

// Database is the selected persistence backend. Values are produced only by
// NewConfig. The unexported method seals the set of implementations to this package.
type Database interface {
	database()
}

// SQLite is file-backed SQLite persistence.
type SQLite struct {
	Path string
}

// Postgres is PostgreSQL persistence parsed from a connection URL.
type Postgres struct {
	Host     string
	Port     int
	User     string
	Password string // secret; never log
	Name     string
	Params   url.Values

	// DriverDSN is the canonical database/sql connection string derived from
	// the fields above at parse time.
	DriverDSN string
}

// String returns a log-safe representation that omits Password and DriverDSN.
func (p Postgres) String() string {
	return fmt.Sprintf("Postgres{Host:%q Port:%d User:%q Name:%q}", p.Host, p.Port, p.User, p.Name)
}

// GoString is the same redacted form used by %#v.
func (p Postgres) GoString() string {
	return p.String()
}

// database seals Database to this package.
func (SQLite) database() {}

// database seals Database to this package.
func (Postgres) database() {}

// databaseKind returns a log-safe label for db without formatting the value
// (Postgres carries Password and DriverDSN).
func databaseKind(db Database) string {
	switch db.(type) {
	case SQLite:
		return "SQLite"
	case Postgres:
		return "Postgres"
	default:
		return "unknown"
	}
}

// ConfigInput is the pure parse input: resolved edge values plus
// explicit-selection metadata. It contains no flag pointers, file handles,
// or other I/O. The CLI/runner edge resolves environment and file indirection
// before constructing this value.
type ConfigInput struct {
	GRPCAddr                      string
	HTTPAddr                      string
	DBPath                        string
	DatabaseURL                   string // flag/env value only (not file content)
	DatabaseURLFileContent        string // content read at the edge; empty when unused
	DatabaseURLFileSet            bool   // file path was non-empty (flag/env)
	DBExplicit                    bool
	OIDCCABundle                  []byte
	WebDir                        string
	OIDCIssuer                    string
	OIDCUIClientID                string
	OIDCUIScope                   string
	OIDCResourceAudience          string
	OIDCKeyEnrollmentAudience     string
	OIDCRegistryID                string
	OIDCRegistrySubjectExpression string
	OIDCPublicKeyClaimExpression  string
	Addons                        string
	GCPHCPConfigPath              string
	UIOrigin                      string
}

// Config is the normalized typed production configuration accepted by
// bootstrap. Obtain it only through NewConfig, which parses ConfigInput and
// returns a value that already satisfies configuration invariants. Runtime
// objects, listeners, loggers, fakes, and controllers are not configuration
// fields. Fields remain exported for composition ergonomics; the supported
// construction path is NewConfig.
type Config struct {
	GRPCAddr string
	HTTPAddr string

	// Database is required and non-nil after NewConfig succeeds.
	Database Database

	// OIDCCABundle is optional PEM-encoded CA material for OIDC issuers.
	// Empty means the system trust store.
	OIDCCABundle []byte

	WebDir         string
	OIDCIssuer     string
	OIDCUIClientID string
	// OIDCUIScope is the browser OIDC scope string advertised on /api/ui/config.
	// Packaging/deploy supplies it; NewConfig does not invent a value.
	OIDCUIScope string

	// OIDC AuthMethod policy for empty-store install. Supplied by the serve
	// caller; NewConfig does not invent values when these are empty.
	OIDCResourceAudience          string
	OIDCKeyEnrollmentAudience     string
	OIDCRegistryID                string
	OIDCRegistrySubjectExpression string
	OIDCPublicKeyClaimExpression  string

	// Addons is the enabled addon name list (order preserved, duplicates removed).
	Addons []AddonName

	// GCPHCPConfigPath is required when AddonGCPHCP is enabled.
	GCPHCPConfigPath string

	// UIOrigin is an optional trusted public origin advertised on
	// /api/ui/config. Empty means derive from HTTPAddr. Must be an absolute
	// http or https origin with no userinfo, query, fragment, or path other
	// than "/".
	UIOrigin string
}

// NewConfig parses resolved edge input into a normalized Config. It performs
// no I/O or resource acquisition. On success the returned Config satisfies
// listen-address, database-selection, URL, CA, addon, and GCP path invariants.
func NewConfig(in ConfigInput) (Config, error) {
	if in.DatabaseURLFileSet && in.DatabaseURL != "" {
		return Config{}, fmt.Errorf("--database-url-file and --database-url are mutually exclusive")
	}
	if in.DatabaseURLFileSet && in.DBExplicit {
		return Config{}, fmt.Errorf("--database-url-file and --db are mutually exclusive")
	}

	effectiveURL := in.DatabaseURL
	if in.DatabaseURLFileSet {
		effectiveURL = in.DatabaseURLFileContent
		// File path selected but empty/whitespace must not fall through to the
		// default SQLite --db path (e.g. an unpopulated mounted secret).
		if strings.TrimSpace(effectiveURL) == "" {
			return Config{}, fmt.Errorf("--database-url-file is set but contains no database URL")
		}
	}
	if effectiveURL != "" && in.DBExplicit {
		return Config{}, fmt.Errorf("--database-url and --db are mutually exclusive")
	}

	var database Database
	switch {
	case effectiveURL != "":
		pg, err := parsePostgresURL(effectiveURL)
		if err != nil {
			return Config{}, err
		}
		database = pg
	case strings.TrimSpace(in.DBPath) != "":
		database = SQLite{Path: in.DBPath}
	default:
		return Config{}, fmt.Errorf("exactly one of SQLite path or database URL is required")
	}

	cfg := Config{
		GRPCAddr:                      in.GRPCAddr,
		HTTPAddr:                      in.HTTPAddr,
		Database:                      database,
		OIDCCABundle:                  append([]byte(nil), in.OIDCCABundle...),
		WebDir:                        in.WebDir,
		OIDCIssuer:                    strings.TrimSpace(in.OIDCIssuer),
		OIDCUIClientID:                strings.TrimSpace(in.OIDCUIClientID),
		OIDCUIScope:                   strings.TrimSpace(in.OIDCUIScope),
		OIDCResourceAudience:          strings.TrimSpace(in.OIDCResourceAudience),
		OIDCKeyEnrollmentAudience:     strings.TrimSpace(in.OIDCKeyEnrollmentAudience),
		OIDCRegistryID:                strings.TrimSpace(in.OIDCRegistryID),
		OIDCRegistrySubjectExpression: strings.TrimSpace(in.OIDCRegistrySubjectExpression),
		OIDCPublicKeyClaimExpression:  strings.TrimSpace(in.OIDCPublicKeyClaimExpression),
		Addons:                        normalizeAddonList(in.Addons),
		GCPHCPConfigPath:              in.GCPHCPConfigPath,
	}
	if uiOrigin := strings.TrimSpace(in.UIOrigin); uiOrigin != "" {
		canonical, err := parseUIOrigin(uiOrigin)
		if err != nil {
			return Config{}, err
		}
		cfg.UIOrigin = canonical
	}
	if err := cfg.checkInvariants(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// AddonSet returns the enabled addons as a set for lifecycle wiring.
func (c Config) AddonSet() map[AddonName]bool {
	out := make(map[AddonName]bool, len(c.Addons))
	for _, name := range c.Addons {
		out[name] = true
	}
	return out
}

// OIDCInitialAuthMethodConfigured reports whether empty-store AuthMethod install has
// the required inputs (issuer + resource audience).
func (c Config) OIDCInitialAuthMethodConfigured() bool {
	return c.OIDCIssuer != "" && c.OIDCResourceAudience != ""
}

// checkInvariants verifies Config invariants after mapping. It performs no I/O
// or resource acquisition.
func (c Config) checkInvariants() error {
	if strings.TrimSpace(c.GRPCAddr) == "" {
		return fmt.Errorf("grpc listen address is required")
	}
	if strings.TrimSpace(c.HTTPAddr) == "" {
		return fmt.Errorf("http listen address is required")
	}
	if c.Database == nil {
		return fmt.Errorf("database configuration is required")
	}
	switch db := c.Database.(type) {
	case SQLite:
		if strings.TrimSpace(db.Path) == "" {
			return fmt.Errorf("SQLite path is required")
		}
	case Postgres:
		if db.Host == "" || db.Name == "" || db.Port <= 0 || db.DriverDSN == "" {
			return fmt.Errorf("incomplete PostgreSQL configuration")
		}
	default:
		return fmt.Errorf("unsupported database type %s", databaseKind(c.Database))
	}

	if len(c.OIDCCABundle) > 0 {
		if err := parseCAPEM(c.OIDCCABundle); err != nil {
			return err
		}
	}

	if c.OIDCIssuer != "" {
		if err := parseOIDCIssuerURL(c.OIDCIssuer); err != nil {
			return err
		}
	}

	regIDSet := c.OIDCRegistryID != ""
	regExprSet := c.OIDCRegistrySubjectExpression != ""
	if regIDSet != regExprSet {
		return fmt.Errorf("oidc registry id and registry subject expression must both be set or both be empty")
	}
	if c.OIDCPublicKeyClaimExpression != "" && (regIDSet || regExprSet) {
		return fmt.Errorf("oidc public-key claim expression and registry subject mapping are mutually exclusive")
	}

	gcphcpEnabled := false
	for _, name := range c.Addons {
		if _, ok := knownAddons[name]; !ok {
			return fmt.Errorf("unknown addon %q", name)
		}
		if name == AddonGCPHCP {
			gcphcpEnabled = true
		}
	}
	if gcphcpEnabled && c.GCPHCPConfigPath == "" {
		return fmt.Errorf("gcphcp addon is enabled but no config was provided; set --gcphcp-config or GCPHCP_CONFIG to a gcphcp.yaml path")
	}

	return nil
}

// normalizeAddonList splits a comma-separated addon wire string into a stable
// list with empty entries dropped and duplicates removed (first wins).
func normalizeAddonList(spec string) []AddonName {
	if spec == "" {
		return nil
	}
	seen := make(map[AddonName]struct{})
	var out []AddonName
	for _, a := range strings.Split(spec, ",") {
		name := AddonName(strings.TrimSpace(a))
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

// parsePostgresURL parses a postgres or postgresql URL into Postgres fields and
// a derived DriverDSN.
func parsePostgresURL(raw string) (Postgres, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return Postgres{}, fmt.Errorf("invalid database URL: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	switch scheme {
	case "postgres", "postgresql":
	default:
		return Postgres{}, fmt.Errorf("invalid database URL: scheme must be postgres or postgresql")
	}
	host := u.Hostname()
	if host == "" {
		return Postgres{}, fmt.Errorf("invalid database URL: host is required")
	}
	port := 5432
	if portStr := u.Port(); portStr != "" {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return Postgres{}, fmt.Errorf("invalid database URL: parse port %q: %w", portStr, err)
		}
	}
	name := strings.TrimPrefix(u.Path, "/")
	if name == "" {
		return Postgres{}, fmt.Errorf("invalid database URL: database name is required")
	}

	password, _ := u.User.Password()
	pg := Postgres{
		Host:     host,
		Port:     port,
		User:     u.User.Username(),
		Password: password,
		Name:     name,
		Params:   u.Query(),
	}
	pg.DriverDSN = pg.driverDSN(scheme)
	return pg, nil
}

// driverDSN builds the canonical database/sql connection string from parsed
// Postgres fields, using scheme (postgres or postgresql).
func (p Postgres) driverDSN(scheme string) string {
	u := url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(p.Host, strconv.Itoa(p.Port)),
		Path:   "/" + p.Name,
	}
	switch {
	case p.Password != "":
		u.User = url.UserPassword(p.User, p.Password)
	case p.User != "":
		u.User = url.User(p.User)
	}
	if len(p.Params) > 0 {
		u.RawQuery = p.Params.Encode()
	}
	return u.String()
}

// parseOIDCIssuerURL validates that raw is an http or https URL with a host.
// HTTPS is allowed for any host. HTTP is allowed only for loopback hosts
// (localhost / 127.0.0.0/8 / ::1) so remote cleartext discovery and JWKS
// fetches cannot be MITMed on the network.
func parseOIDCIssuerURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid oidc issuer: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	switch scheme {
	case "http", "https":
	default:
		return fmt.Errorf("invalid oidc issuer: scheme must be http or https")
	}
	if u.Host == "" {
		return fmt.Errorf("invalid oidc issuer: host is required")
	}
	if scheme == "http" && !isLoopbackHost(u.Hostname()) {
		return fmt.Errorf("invalid oidc issuer: http is only allowed for loopback hosts")
	}
	return nil
}

// isLoopbackHost reports whether host (no port) is a loopback name or address.
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// parseUIOrigin validates that raw is an absolute http or https origin with no
// userinfo, query, fragment, or path other than "/". The returned value has
// no trailing slash.
func parseUIOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid ui origin: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	switch scheme {
	case "http", "https":
	default:
		return "", fmt.Errorf("invalid ui origin: scheme must be http or https")
	}
	if u.User != nil {
		return "", fmt.Errorf("invalid ui origin: userinfo is not allowed")
	}
	if u.RawQuery != "" {
		return "", fmt.Errorf("invalid ui origin: query is not allowed")
	}
	if u.Fragment != "" {
		return "", fmt.Errorf("invalid ui origin: fragment is not allowed")
	}
	if u.Host == "" {
		return "", fmt.Errorf("invalid ui origin: host is required")
	}
	if u.Path != "" && u.Path != "/" {
		return "", fmt.Errorf("invalid ui origin: path must be empty or /")
	}
	return scheme + "://" + u.Host, nil
}

// parseCAPEM requires data to contain at least one parseable PEM CERTIFICATE
// block.
func parseCAPEM(data []byte) error {
	rest := data
	found := false
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return fmt.Errorf("invalid OIDC CA data: %w", err)
		}
		found = true
	}
	if !found {
		return fmt.Errorf("invalid OIDC CA data: no PEM CERTIFICATE blocks found")
	}
	return nil
}
