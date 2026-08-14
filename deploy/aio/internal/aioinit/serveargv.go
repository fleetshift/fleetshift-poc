package aioinit

import (
	"os"
	"path/filepath"
	"strings"
)

const (
	// DefaultUIClientID is the browser OIDC client ID.
	DefaultUIClientID = "fleetshift-ui"
	// DefaultUIScope is the browser OIDC scope string.
	DefaultUIScope = "openid profile email groups audience:server:client_id:fleetshift"
	// DefaultResourceAudience is the ordinary AuthMethod audience.
	DefaultResourceAudience = "fleetshift"
	// DefaultEnrollmentAudience is the signing enrollment audience.
	DefaultEnrollmentAudience = "fleetshift-signing"
	// DefaultRegistryID is the default external key registry.
	DefaultRegistryID = "github.com"
	// DefaultRegistrySubjectExpression maps preferred_username.
	DefaultRegistrySubjectExpression = "claims.preferred_username"

	defaultServeDB     = "/data/fleetshift.db"
	defaultServeWebDir = "/srv/web"
	// defaultServeLogLevel is used when FLEETSHIFT_LOG_LEVEL is unset.
	defaultServeLogLevel = "debug"

	// ServeExecPath is where aio-init writes the executable serve wrapper.
	ServeExecPath = "/run/fleetshift/exec-serve"
)

// ServeConfig is packaging-resolved configuration for `fleetshift serve`.
type ServeConfig struct {
	Endpoints          Endpoints
	Issuer             string // peer or external
	CAFile             string // sandbox CA on Dex-on; optional OIDC_CA_FILE on Dex-off
	UIClientID         string
	UIScope            string
	ResourceAudience   string
	EnrollmentAudience string
	RegistryID         string
	RegistryExpr       string
	PublicKeyExpr      string
	DBPath             string
	WebDir             string
	LogLevel           string
	Addons             string
	GCPHCPConfig       string
}

// ApplyServeDefaults fills packaging defaults for omitted AuthMethod/UI fields.
// OIDC wire invariants (issuer URL, CA, registry pairing) are enforced by
// fleetshift serve when the generated argv runs.
func ApplyServeDefaults(in ServeConfig) ServeConfig {
	if in.UIClientID == "" {
		in.UIClientID = DefaultUIClientID
	}
	if in.UIScope == "" {
		in.UIScope = DefaultUIScope
	}
	if in.ResourceAudience == "" {
		in.ResourceAudience = DefaultResourceAudience
	}
	if in.EnrollmentAudience == "" {
		in.EnrollmentAudience = DefaultEnrollmentAudience
	}
	if in.PublicKeyExpr == "" {
		if in.RegistryID == "" {
			in.RegistryID = DefaultRegistryID
		}
		if in.RegistryExpr == "" {
			in.RegistryExpr = DefaultRegistrySubjectExpression
		}
	}
	if in.DBPath == "" {
		in.DBPath = defaultServeDB
	}
	if in.WebDir == "" {
		in.WebDir = defaultServeWebDir
	}
	if in.LogLevel == "" {
		in.LogLevel = defaultServeLogLevel
	}
	return in
}

// ServeArgs returns the argv for `fleetshift` including the serve subcommand.
func ServeArgs(in ServeConfig) []string {
	args := []string{
		"serve",
		"--http-addr", in.Endpoints.HTTPListen,
		"--grpc-addr", in.Endpoints.GRPCListen,
		"--db", in.DBPath,
		"--web-dir", in.WebDir,
		"--log-level", in.LogLevel,
		"--oidc-issuer", in.Issuer,
		"--oidc-resource-audience", in.ResourceAudience,
		"--oidc-ui-client-id", in.UIClientID,
		"--oidc-ui-scope", in.UIScope,
	}
	if in.EnrollmentAudience != "" {
		args = append(args, "--oidc-key-enrollment-audience", in.EnrollmentAudience)
	}
	if in.RegistryID != "" && in.RegistryExpr != "" {
		args = append(args,
			"--oidc-registry-id", in.RegistryID,
			"--oidc-registry-subject-expression", in.RegistryExpr,
		)
	}
	if in.PublicKeyExpr != "" {
		args = append(args, "--oidc-public-key-claim-expression", in.PublicKeyExpr)
	}
	if in.CAFile != "" {
		args = append(args, "--oidc-ca-file", in.CAFile)
	}
	if in.Addons != "" {
		args = append(args, "--addons", in.Addons)
	}
	if in.GCPHCPConfig != "" {
		args = append(args, "--gcphcp-config", in.GCPHCPConfig)
	}
	return args
}

// WriteServeExecScript atomically writes an executable fleetshift serve wrapper at path.
func WriteServeExecScript(path string, args []string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("#!/bin/sh\n")
	b.WriteString("exec /usr/local/bin/fleetshift")
	for _, a := range args {
		b.WriteByte(' ')
		b.WriteString(shellQuote(a))
	}
	b.WriteByte('\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(b.String()), 0755); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// shellQuote returns s as a single-quoted shell word safe for /bin/sh.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
