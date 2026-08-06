package dexidp

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	dex "github.com/dexidp/dex/server"
	"github.com/dexidp/dex/server/signer"
	"github.com/dexidp/dex/storage"
	"github.com/dexidp/dex/storage/sql"
	_ "github.com/mattn/go-sqlite3" // Blank import required to register sqlite3 driver
	"github.com/spf13/cobra"
	"golang.org/x/crypto/bcrypt"
)

func newDexCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "dex",
		Short: "Start the Dex IDP server",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			handler, err := StartDexIDP(ctx)
			if err != nil {
				return err
			}

			addr := ":5556"
			srv := &http.Server{Addr: addr, Handler: handler}

			go func() {
				<-ctx.Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				srv.Shutdown(shutdownCtx)
			}()

			fmt.Printf("dex listening on %s\n", addr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				return err
			}
			return nil
		},
	}
}

func StartDexIDP(ctx context.Context) (http.Handler, error) {
	dbConfig := &sql.SQLite3{
		File: "dex.db",
	}

	logger := slog.Default()
	store, err := dbConfig.Open(logger)

	if err != nil {
		return nil, fmt.Errorf("failed to open sql storage: %w", err)
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost) // Replace with a secure password in production
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	// replace with configureable values in production
	OAuthDefaults := storage.Client{
		ID: "fleetshift-ui",
		// may need some updastes to cli or keep as "http://127.0.0.1:*" which is
		// RedirectURIs: []string{
		// 	// for UI
		// 	"http://localhost:8085/auth/callback",
		// 	"http://localhost:8085/silent-renew.html",
		// 	// for CLI
		// 	"http://127.0.0.1/callback",
		// },
		Public: true,
		Name:   "ACME Corp",
	}

	staticPassword := storage.Password{
		Email:    "admin@email.com",
		Hash:     hashedPassword,
		Username: "admin",
		UserID:   "00000000-0000-0000-0000-000000000001",
	}

	_, err = store.GetClient(ctx, OAuthDefaults.ID)
	if err != nil {
		if err == storage.ErrNotFound {
			err = store.CreateClient(ctx, OAuthDefaults)
			if err != nil {
				return nil, fmt.Errorf("failed to create OAuth client: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to get OAuth client: %w", err)
		}
	}

	_, err = store.GetPassword(ctx, staticPassword.Email)
	if err != nil {
		if err == storage.ErrNotFound {
			err = store.CreatePassword(ctx, staticPassword)
			if err != nil {
				return nil, fmt.Errorf("failed to create static password: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to get static password: %w", err)
		}
	}

	staticConnector := storage.Connector{
		ID:   "local",
		Type: "local",
		Name: "Email",
	}

	_, err = store.GetConnector(ctx, staticConnector.ID)
	if err != nil {
		if err == storage.ErrNotFound {
			err = store.CreateConnector(ctx, staticConnector)
			if err != nil {
				return nil, fmt.Errorf("failed to create static connector: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to get static connector: %w", err)
		}
	}

	mockSigner, err := signer.NewMockSigner(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create mock signer: %w", err)
	}

	serverConfig := dex.Config{
		Issuer:                 "http://localhost:5556/dex",
		Storage:                store,
		SupportedResponseTypes: []string{"code", "token", "id_token"},
		AllowedOrigins: []string{
			"http://localhost:8085",
		},
		SkipApprovalScreen: true,
		Logger:             logger,
		PasswordConnector:  "local",
		Signer:             mockSigner,
	}

	dexServer, err := dex.NewServer(ctx, serverConfig)
	if err != nil {
		return nil, err
	}

	return dexServer, nil
}

func New() *cobra.Command {
	root := &cobra.Command{
		Use:   "fleetshift",
		Short: "FleetShift management plane",
	}

	root.AddCommand(newDexCmd())

	return root
}
