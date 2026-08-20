package auth_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func TestFileStore_SaveAndLoad(t *testing.T) {
	ctx := context.Background()
	store := auth.FileStore{Dir: t.TempDir()}

	tokens := auth.Tokens{
		AccessToken:  "access-123",
		RefreshToken: "refresh-456",
		IDToken:      "id-token-789",
		Expiry:       time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC),
		TokenType:    "Bearer",
	}

	if err := store.Save(ctx, tokens); err != nil {
		t.Fatalf("Save: %v", err)
	}

	info, err := os.Stat(filepath.Join(store.Dir, "credentials.json"))
	if err != nil {
		t.Fatalf("stat credentials: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("credentials mode = %o, want 0600", info.Mode().Perm())
	}

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.AccessToken != tokens.AccessToken {
		t.Errorf("AccessToken: got %q, want %q", loaded.AccessToken, tokens.AccessToken)
	}
	if loaded.RefreshToken != tokens.RefreshToken {
		t.Errorf("RefreshToken: got %q, want %q", loaded.RefreshToken, tokens.RefreshToken)
	}
	if loaded.IDToken != tokens.IDToken {
		t.Errorf("IDToken: got %q, want %q", loaded.IDToken, tokens.IDToken)
	}
	if loaded.TokenType != tokens.TokenType {
		t.Errorf("TokenType: got %q, want %q", loaded.TokenType, tokens.TokenType)
	}
	if !loaded.Expiry.Equal(tokens.Expiry) {
		t.Errorf("Expiry: got %v, want %v", loaded.Expiry, tokens.Expiry)
	}
}

func TestFileStore_LoadEmpty(t *testing.T) {
	store := auth.FileStore{Dir: t.TempDir()}
	if _, err := store.Load(context.Background()); err == nil {
		t.Fatal("Load: expected error for empty store, got nil")
	}
}

func TestFileStore_Clear(t *testing.T) {
	ctx := context.Background()
	store := auth.FileStore{Dir: t.TempDir()}

	if err := store.Save(ctx, auth.Tokens{AccessToken: "access-123", TokenType: "Bearer"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := store.Clear(ctx); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("Load after Clear: expected error, got nil")
	}
	if err := store.Clear(ctx); err != nil {
		t.Fatalf("Clear missing file: %v", err)
	}
}

func TestFileStore_SigningKeyRoundTrip(t *testing.T) {
	store := auth.FileStore{Dir: t.TempDir()}
	const pem = "-----BEGIN EC PRIVATE KEY-----\ntest\n-----END EC PRIVATE KEY-----\n"
	if err := store.SaveSigningKey(pem); err != nil {
		t.Fatalf("SaveSigningKey: %v", err)
	}
	got, err := store.LoadSigningKey()
	if err != nil {
		t.Fatalf("LoadSigningKey: %v", err)
	}
	if got != pem {
		t.Errorf("LoadSigningKey = %q, want %q", got, pem)
	}

	info, err := os.Stat(filepath.Join(store.Dir, "signing_key.pem"))
	if err != nil {
		t.Fatalf("stat signing key: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("signing_key.pem mode = %o, want 0600", info.Mode().Perm())
	}
}

func TestFileStore_CreatesDirMode0700(t *testing.T) {
	store := auth.FileStore{Dir: filepath.Join(t.TempDir(), "state")}
	if err := store.Save(context.Background(), auth.Tokens{AccessToken: "tok", TokenType: "Bearer"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	info, err := os.Stat(store.Dir)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Errorf("config dir mode = %o, want 0700", info.Mode().Perm())
	}
}

func TestFileStore_EmptyDirDoesNotWriteCWD(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	store := auth.FileStore{}
	ctx := context.Background()
	if err := store.Save(ctx, auth.Tokens{AccessToken: "tok", TokenType: "Bearer"}); err == nil {
		t.Fatal("Save with empty Dir: expected error")
	}
	if err := store.SaveSigningKey("pem"); err == nil {
		t.Fatal("SaveSigningKey with empty Dir: expected error")
	}
	entries, err := os.ReadDir(cwd)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("cwd was mutated: %v", names(entries))
	}
}

func TestFileStore_LoadInvalidJSON(t *testing.T) {
	store := auth.FileStore{Dir: t.TempDir()}
	path := filepath.Join(store.Dir, "credentials.json")
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := store.Load(context.Background()); err == nil {
		t.Fatal("Load: expected parse error")
	}
}

func TestFileStore_LoadSigningKeyMissing(t *testing.T) {
	store := auth.FileStore{Dir: t.TempDir()}
	if _, err := store.LoadSigningKey(); err == nil {
		t.Fatal("LoadSigningKey: expected error for missing file")
	}
}

func TestFileStore_ClearLeavesSigningKey(t *testing.T) {
	ctx := context.Background()
	store := auth.FileStore{Dir: t.TempDir()}
	const pem = "-----BEGIN EC PRIVATE KEY-----\ntest\n-----END EC PRIVATE KEY-----\n"
	if err := store.Save(ctx, auth.Tokens{AccessToken: "tok", TokenType: "Bearer"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := store.SaveSigningKey(pem); err != nil {
		t.Fatalf("SaveSigningKey: %v", err)
	}
	if err := store.Clear(ctx); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("Load after Clear: expected error")
	}
	got, err := store.LoadSigningKey()
	if err != nil {
		t.Fatalf("LoadSigningKey after Clear: %v", err)
	}
	if got != pem {
		t.Fatalf("signing key after Clear = %q, want preserved PEM", got)
	}
}

func TestFileStore_DoesNotTouchHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg-config"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, "xdg-state"))

	ctx := context.Background()
	store := auth.FileStore{Dir: t.TempDir()}
	if err := store.Save(ctx, auth.Tokens{AccessToken: "tok", TokenType: "Bearer"}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	entries, err := os.ReadDir(home)
	if err != nil {
		t.Fatalf("ReadDir home: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("home was mutated: %v", names(entries))
	}
}

func names(entries []os.DirEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Name()
	}
	return out
}
