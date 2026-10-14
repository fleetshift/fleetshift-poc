package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/bootstrap"
)

// serveSelections records which flags were explicitly set via the CLI
// (pflag.Changed). Environment-provided defaults are not explicit.
type serveSelections struct {
	DBExplicit bool
}

// loadServeConfig resolves environment/file inputs without mutating serveFlags,
// then parses them into bootstrap.Config before any resource acquisition.
func loadServeConfig(f *serveFlags, sel serveSelections) (bootstrap.Config, error) {
	var dbURLFromFile string
	if f.databaseURLFile != "" {
		content, err := readDatabaseURLFile(f.databaseURLFile)
		if err != nil {
			return bootstrap.Config{}, err
		}
		dbURLFromFile = content
	}

	var caBundle []byte
	if f.oidcCAFile != "" {
		data, err := os.ReadFile(f.oidcCAFile)
		if err != nil {
			return bootstrap.Config{}, fmt.Errorf("read OIDC CA file: %w", err)
		}
		caBundle = data
	}

	return bootstrap.NewConfig(bootstrap.ConfigInput{
		GRPCAddr:                      f.grpcAddr,
		HTTPAddr:                      f.httpAddr,
		DBPath:                        f.dbPath,
		DatabaseURL:                   f.databaseURL,
		DatabaseURLFileContent:        dbURLFromFile,
		DatabaseURLFileSet:            f.databaseURLFile != "",
		DBExplicit:                    sel.DBExplicit,
		OIDCCABundle:                  caBundle,
		WebDir:                        f.webDir,
		OIDCIssuer:                    f.oidcIssuer,
		OIDCUIClientID:                f.oidcUIClientID,
		OIDCUIScope:                   f.oidcUIScope,
		OIDCResourceAudience:          f.oidcResourceAudience,
		OIDCKeyEnrollmentAudience:     f.oidcKeyEnrollmentAudience,
		OIDCRegistryID:                f.oidcRegistryID,
		OIDCRegistrySubjectExpression: f.oidcRegistrySubjectExpression,
		OIDCPublicKeyClaimExpression:  f.oidcPublicKeyClaimExpression,
		Addons:                        f.addons,
		GCPHCPConfigPath:              resolveGCPHCPConfigPath(f.gcphcpConfig),
	})
}

// readDatabaseURLFile reads path and returns its trimmed PostgreSQL URL contents.
func readDatabaseURLFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read database URL file: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}
