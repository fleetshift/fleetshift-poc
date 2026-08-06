package gcphcp

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"
)

const stsGrantType = "urn:ietf:params:oauth:grant-type:token-exchange"

const (
	stsRequestedTokenType       = "urn:ietf:params:oauth:token-type:access_token"
	stsSubjectTokenType         = "urn:ietf:params:oauth:token-type:jwt"
	stsScope                    = "https://www.googleapis.com/auth/cloud-platform"
	localSTSReadHeaderTimeout   = 5 * time.Second
	localSTSIdleTimeout         = 30 * time.Second
	localSTSMaxHeaderBytes      = 8 << 10
	localSTSPathSuffixByteCount = 8
)

// localSTSForwarder serves a loopback-only STS-compatible endpoint for
// hypershift create and destroy flows. FleetShift already has a fresher
// workforce token than the original caller JWT at that point, so this shim
// lets hypershift fetch the cached token through the ADC contract it already
// understands.
//
// TODO: Remove this shim once we implement the hypershift-side create/destroy
// logic directly instead of translating through a local STS-compatible
// endpoint.
type localSTSForwarder struct {
	listener net.Listener
	server   *http.Server
	path     string

	token        string
	expiry       time.Time
	subjectToken string
	audience     string
}

// randomHexString returns a cryptographically random hex string with the
// requested byte-count of entropy.
func randomHexString(byteCount int) (string, error) {
	buf := make([]byte, byteCount)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// startLocalSTSForwarder starts a loopback-only STS-compatible endpoint that
// returns the cached workforce token for hypershift create/destroy calls.
func startLocalSTSForwarder(token string, expiry time.Time, subjectToken string, audience string) (*localSTSForwarder, error) {
	if subjectToken == "" {
		return nil, fmt.Errorf("subject token is required")
	}
	if audience == "" {
		return nil, fmt.Errorf("audience is required")
	}

	pathSuffix, err := randomHexString(localSTSPathSuffixByteCount)
	if err != nil {
		return nil, fmt.Errorf("generate forwarder path: %w", err)
	}

	forwarder := &localSTSForwarder{
		path:         "/sts/" + pathSuffix,
		token:        token,
		expiry:       expiry,
		subjectToken: subjectToken,
		audience:     audience,
	}

	mux := http.NewServeMux()
	mux.HandleFunc(forwarder.path, forwarder.handleSTS)
	forwarder.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: localSTSReadHeaderTimeout,
		IdleTimeout:       localSTSIdleTimeout,
		MaxHeaderBytes:    localSTSMaxHeaderBytes,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen on loopback: %w", err)
	}
	forwarder.listener = listener

	go func() {
		_ = forwarder.server.Serve(listener)
	}()

	return forwarder, nil
}

// URL returns the full loopback URL for the forwarder's STS endpoint.
func (f *localSTSForwarder) URL() string {
	return "http://" + f.listener.Addr().String() + f.path
}

// Close gracefully shuts down the loopback forwarder.
func (f *localSTSForwarder) Close() error {
	if f == nil || f.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return f.server.Shutdown(ctx)
}

// handleSTS validates the expected STS exchange request and returns the cached
// workforce token in Google's STS response shape.
func (f *localSTSForwarder) handleSTS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form body", http.StatusBadRequest)
		return
	}
	if r.Form.Get("grant_type") != stsGrantType {
		http.Error(w, "invalid grant_type", http.StatusBadRequest)
		return
	}
	if r.Form.Get("requested_token_type") != stsRequestedTokenType {
		http.Error(w, "invalid requested_token_type", http.StatusBadRequest)
		return
	}
	if r.Form.Get("subject_token_type") != stsSubjectTokenType {
		http.Error(w, "invalid subject_token_type", http.StatusBadRequest)
		return
	}
	if r.Form.Get("audience") != f.audience {
		http.Error(w, "invalid audience", http.StatusBadRequest)
		return
	}
	if r.Form.Get("scope") != stsScope {
		http.Error(w, "invalid scope", http.StatusBadRequest)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Form.Get("subject_token")), []byte(f.subjectToken)) != 1 {
		http.Error(w, "invalid subject_token", http.StatusUnauthorized)
		return
	}

	expiresIn := int(time.Until(f.expiry).Seconds())
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if expiresIn <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":             "invalid_grant",
			"error_description": "cached workforce token expired",
		})
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token":      f.token,
		"token_type":        "Bearer",
		"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
		"expires_in":        expiresIn,
	})
}
