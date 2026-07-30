package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/slogutil"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/serverapp"
)

// serveFlags holds raw serve CLI flag values before edge resolution into
// serverapp.Config.
type serveFlags struct {
	grpcAddr         string
	httpAddr         string
	dbPath           string
	databaseURL      string
	databaseURLFile  string
	logLevel         string
	logFormat        string
	logLevelOverride string
	oidcCAFile       string
	webDir           string
	oidcUIAuthority  string
	oidcUIClientID   string
	addons           string
	gcphcpConfig     string
}

// newServeCmd builds the serve Cobra command and passes explicit --db selection
// into runServe.
func newServeCmd() *cobra.Command {
	f := &serveFlags{}
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the FleetShift gRPC and HTTP servers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			sel := serveSelections{DB: cmd.Flags().Changed("db")}
			return runServe(cmd.Context(), f, sel)
		},
	}
	cmd.Flags().StringVar(&f.grpcAddr, "grpc-addr", ":50051", "gRPC listen address")
	cmd.Flags().StringVar(&f.httpAddr, "http-addr", ":8080", "HTTP/JSON gateway listen address")
	cmd.Flags().StringVar(&f.dbPath, "db", serverapp.DefaultSQLitePath, "SQLite database path")
	cmd.Flags().StringVar(&f.databaseURL, "database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL (mutually exclusive with --db)")
	cmd.Flags().StringVar(&f.databaseURLFile, "database-url-file", os.Getenv("DATABASE_URL_FILE"), "path to file containing PostgreSQL connection URL (mutually exclusive with --database-url and --db)")
	cmd.Flags().StringVar(&f.logLevel, "log-level", "info", "log level (debug, info, warn, error)")
	cmd.Flags().StringVar(&f.logFormat, "log-format", "text", "log format (text, json)")
	cmd.Flags().StringVar(&f.logLevelOverride, "log-level-override", "", "per-component log level overrides (e.g. deployment=debug,authn=debug)")
	cmd.Flags().StringVar(&f.oidcCAFile, "oidc-ca-file", "", "PEM CA certificate for OIDC issuers (for kind clusters trusting self-signed or local CAs)")
	cmd.Flags().StringVar(&f.webDir, "web-dir", "", "directory containing frontend assets to serve (empty = API only)")
	cmd.Flags().StringVar(&f.oidcUIAuthority, "oidc-ui-authority", os.Getenv("OIDC_ISSUER_URL"), "OIDC authority URL for the frontend UI")
	cmd.Flags().StringVar(&f.oidcUIClientID, "oidc-ui-client-id", envOrDefault("OIDC_UI_CLIENT_ID", "fleetshift-ui"), "OIDC client ID for the frontend UI")
	cmd.Flags().StringVar(&f.addons, "addons", defaultAddons(), "comma-separated list of addons to enable (default: kind,kubernetes; override with FLEETSHIFT_SERVER_ADDONS)")
	cmd.Flags().StringVar(&f.gcphcpConfig, "gcphcp-config", "", "path to gcphcp addon config file (or GCPHCP_CONFIG env)")
	return cmd
}

// runServe loads normalized config at the CLI edge, builds the logger, and
// delegates construction, readiness, supervision, and cleanup to serverapp.
func runServe(ctx context.Context, f *serveFlags, sel serveSelections) error {
	signalCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := loadServeConfig(f, sel)
	if err != nil {
		return err
	}

	logger, err := buildLogger(f.logLevel, f.logFormat, f.logLevelOverride)
	if err != nil {
		return err
	}

	app, err := serverapp.Start(signalCtx, cfg, logger)
	if err != nil {
		return err
	}

	waitErr := make(chan error, 1)
	go func() { waitErr <- app.Wait() }()

	select {
	case <-signalCtx.Done():
		logger.Info("shutting down")
	case err := <-waitErr:
		if err != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = app.Close(closeCtx)
			return err
		}
	}

	// Fresh bounded context: never pass the cancelled signal context to Close.
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return app.Close(closeCtx)
}

// buildLogger creates a stderr slog logger with the given base level, format
// (text or json), and optional per-component level overrides.
func buildLogger(level, format, overrideSpec string) (*slog.Logger, error) {
	base, err := parseLevel(level)
	if err != nil {
		return nil, err
	}

	overrides, err := parseLevelOverrides(overrideSpec)
	if err != nil {
		return nil, err
	}

	// The inner handler's level must be the minimum of the base and all
	// overrides so it never prematurely rejects records an override wants.
	innerLevel := base
	for _, lvl := range overrides {
		if lvl < innerLevel {
			innerLevel = lvl
		}
	}

	opts := &slog.HandlerOptions{Level: innerLevel}
	var inner slog.Handler
	switch strings.ToLower(format) {
	case "json":
		inner = slog.NewJSONHandler(os.Stderr, opts)
	case "text", "":
		inner = slog.NewTextHandler(os.Stderr, opts)
	default:
		return nil, fmt.Errorf("unknown log format %q (valid: text, json)", format)
	}

	handler := slogutil.NewLevelOverrideHandler(inner, base, overrides)
	return slog.New(handler), nil
}

// parseLevel maps a level name to slog.Level. An empty string means info.
func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q (valid: debug, info, warn, error)", s)
	}
}

// parseLevelOverrides parses a comma-separated string of component=level
// pairs (e.g. "deployment=debug,authn=warn").
func parseLevelOverrides(spec string) (map[slogutil.ComponentName]slog.Level, error) {
	if spec == "" {
		return nil, nil
	}
	overrides := make(map[slogutil.ComponentName]slog.Level)
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		k, v, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("invalid log level override %q: expected component=level", entry)
		}
		lvl, err := parseLevel(v)
		if err != nil {
			return nil, fmt.Errorf("invalid log level override %q: %w", entry, err)
		}
		overrides[slogutil.ComponentName(k)] = lvl
	}
	return overrides, nil
}

// envOrDefault returns the environment value for key, or fallback when the
// variable is unset or empty.
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// defaultAddons returns the serve --addons default. An explicit --addons flag
// remains authoritative over FLEETSHIFT_SERVER_ADDONS.
func defaultAddons() string {
	return envOrDefault("FLEETSHIFT_SERVER_ADDONS", "kind,kubernetes")
}

// resolveGCPHCPConfigPath returns flagPath when set; otherwise GCPHCP_CONFIG.
func resolveGCPHCPConfigPath(flagPath string) string {
	if flagPath != "" {
		return flagPath
	}
	return os.Getenv("GCPHCP_CONFIG")
}

// parseAddons splits a comma-separated addon wire string into an AddonName set.
// It does not check whether names are allow-listed.
func parseAddons(spec string) map[serverapp.AddonName]bool {
	addons := make(map[serverapp.AddonName]bool)
	if spec == "" {
		return addons
	}
	for a := range strings.SplitSeq(spec, ",") {
		name := serverapp.AddonName(strings.TrimSpace(a))
		if name != "" {
			addons[name] = true
		}
	}
	return addons
}
