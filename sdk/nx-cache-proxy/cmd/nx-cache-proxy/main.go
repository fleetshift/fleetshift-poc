package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	cfg := loadConfig()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))

	minioClient, err := minio.New(cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Secure: cfg.MinioSecure,
	})
	if err != nil {
		logger.Error("failed to create MinIO client", "error", err)
		os.Exit(1)
	}

	proxy := &cacheProxy{
		client:     minioClient,
		bucket:     cfg.MinioBucket,
		readToken:  cfg.ReadToken,
		writeToken: cfg.WriteToken,
		cacheTTL:   cfg.CacheTTL,
		logger:     logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/cache/{hash}", proxy.handleGet)
	mux.HandleFunc("PUT /v1/cache/{hash}", proxy.handlePut)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
		IdleTimeout:  2 * time.Minute,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("starting nx-cache-proxy", "port", cfg.Port, "bucket", cfg.MinioBucket)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "error", err)
	}
}

type cacheProxy struct {
	client     *minio.Client
	bucket     string
	readToken  string
	writeToken string
	cacheTTL   time.Duration
	logger     *slog.Logger
}

func (p *cacheProxy) handleGet(w http.ResponseWriter, r *http.Request) {
	hash := r.PathValue("hash")
	if hash == "" {
		http.Error(w, "missing hash", http.StatusBadRequest)
		return
	}

	if !p.authorize(r, false) {
		http.Error(w, "missing or invalid authentication token", http.StatusUnauthorized)
		return
	}

	obj, err := p.client.GetObject(r.Context(), p.bucket, hash, minio.GetObjectOptions{})
	if err != nil {
		p.logger.Warn("MinIO unreachable, treating as cache miss", "hash", hash, "error", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer obj.Close()

	info, err := obj.Stat()
	if err != nil {
		var minioErr minio.ErrorResponse
		if errors.As(err, &minioErr) && minioErr.Code == "NoSuchKey" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		p.logger.Warn("MinIO unreachable, treating as cache miss", "hash", hash, "error", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if p.cacheTTL > 0 && time.Since(info.LastModified) > p.cacheTTL {
		p.logger.Debug("cache expired", "hash", hash, "age", time.Since(info.LastModified).Round(time.Second))
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	if _, err := io.Copy(w, obj); err != nil {
		p.logger.Warn("response stream interrupted", "hash", hash, "error", err)
	}

	p.logger.Debug("cache hit", "hash", hash, "size", info.Size)
}

func (p *cacheProxy) handlePut(w http.ResponseWriter, r *http.Request) {
	hash := r.PathValue("hash")
	if hash == "" {
		http.Error(w, "missing hash", http.StatusBadRequest)
		return
	}

	token := extractBearer(r)
	if token == "" || (!constantTimeEqual(token, p.readToken) && !constantTimeEqual(token, p.writeToken)) {
		http.Error(w, "missing or invalid authentication token", http.StatusUnauthorized)
		return
	}
	if !constantTimeEqual(token, p.writeToken) {
		http.Error(w, "access forbidden: read-only token cannot write", http.StatusForbidden)
		return
	}

	// Enforce immutability: 409 if hash already exists and is not expired (CREEP mitigation).
	// Expired entries can be overwritten to allow cache refresh.
	existing, err := p.client.StatObject(r.Context(), p.bucket, hash, minio.StatObjectOptions{})
	if err == nil {
		expired := p.cacheTTL > 0 && time.Since(existing.LastModified) > p.cacheTTL
		if !expired {
			w.WriteHeader(http.StatusConflict)
			return
		}
		p.logger.Debug("overwriting expired entry", "hash", hash, "age", time.Since(existing.LastModified).Round(time.Second))
	} else {
		var minioErr minio.ErrorResponse
		if !errors.As(err, &minioErr) || minioErr.Code != "NoSuchKey" {
			p.logger.Warn("MinIO unreachable, accepting write silently", "hash", hash, "error", err)
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	size := r.ContentLength
	if size < 0 {
		http.Error(w, "Content-Length required", http.StatusBadRequest)
		return
	}

	_, err = p.client.PutObject(r.Context(), p.bucket, hash, r.Body, size, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		p.logger.Warn("MinIO unreachable, cache write dropped", "hash", hash, "error", err)
		w.WriteHeader(http.StatusOK)
		return
	}

	p.logger.Info("cache stored", "hash", hash, "size", size)
	w.WriteHeader(http.StatusOK)
}

// authorize checks the bearer token. requireWrite=true demands the write token.
func (p *cacheProxy) authorize(r *http.Request, requireWrite bool) bool {
	token := extractBearer(r)
	if token == "" {
		return false
	}
	if requireWrite {
		return constantTimeEqual(token, p.writeToken)
	}
	// Read access: either token works.
	return constantTimeEqual(token, p.readToken) || constantTimeEqual(token, p.writeToken)
}

func extractBearer(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(auth, "Bearer ")
}

func constantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

type config struct {
	Port           string
	MinioEndpoint  string
	MinioAccessKey string
	MinioSecretKey string
	MinioBucket    string
	MinioSecure    bool
	ReadToken      string
	WriteToken     string
	CacheTTL       time.Duration
	LogLevel       slog.Level
}

func loadConfig() config {
	c := config{
		Port:           envOrDefault("PORT", "8080"),
		MinioEndpoint:  requireEnv("MINIO_ENDPOINT"),
		MinioAccessKey: requireEnv("MINIO_ACCESS_KEY"),
		MinioSecretKey: requireEnv("MINIO_SECRET_KEY"),
		MinioBucket:    envOrDefault("MINIO_BUCKET", "nx-cache"),
		MinioSecure:    envOrDefault("MINIO_SECURE", "false") == "true",
		ReadToken:      requireEnv("NX_CACHE_READ_TOKEN"),
		WriteToken:     requireEnv("NX_CACHE_WRITE_TOKEN"),
		LogLevel:       slog.LevelInfo,
	}
	if ttl := envOrDefault("CACHE_TTL", "24h"); ttl != "" {
		d, err := time.ParseDuration(ttl)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid CACHE_TTL %q: %v\n", ttl, err)
			os.Exit(1)
		}
		c.CacheTTL = d
	}
	if envOrDefault("LOG_LEVEL", "info") == "debug" {
		c.LogLevel = slog.LevelDebug
	}
	return c
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "required env var %s is not set\n", key)
		os.Exit(1)
	}
	return v
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
