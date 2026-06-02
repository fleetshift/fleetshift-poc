package http

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// VerifySignHandler verifies a detached ECDSA signature against the
// caller's enrolled signing key.
type VerifySignHandler struct {
	AuthMethods   *application.AuthMethodService
	Verifier      domain.OIDCTokenVerifier
	Store         domain.Store
	ProvenanceSvc *domain.ProvenanceService
}

type verifySignRequest struct {
	Payload   string `json:"payload"`
	Signature string `json:"signature"`
}

type verifySignResponse struct {
	Verified bool   `json:"verified"`
	Error    string `json:"error,omitempty"`
}

func (h *VerifySignHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token := extractBearer(r)
	if token == "" {
		writeJSON(w, http.StatusUnauthorized, verifySignResponse{Error: "missing Authorization header"})
		return
	}

	methods, err := h.AuthMethods.List(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, verifySignResponse{Error: "load auth methods"})
		return
	}

	var caller *domain.SubjectClaims
	for _, m := range methods {
		if m.Type != domain.AuthMethodTypeOIDC || m.OIDC == nil {
			continue
		}
		// Verify against the enrollment audience — the UI sends an ID token
		// (aud = OIDC client ID) for signing operations.
		enrollConfig := *m.OIDC
		enrollConfig.Audience = m.OIDC.KeyEnrollmentAudience
		claims, verifyErr := h.Verifier.Verify(r.Context(), enrollConfig, token)
		if verifyErr != nil {
			writeJSON(w, http.StatusUnauthorized, verifySignResponse{Error: verifyErr.Error()})
			return
		}
		caller = &claims
		break
	}

	if caller == nil {
		writeJSON(w, http.StatusUnauthorized, verifySignResponse{Error: "no matching auth method"})
		return
	}

	var req verifySignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, verifySignResponse{Error: "invalid request body"})
		return
	}

	sigBytes, err := base64.StdEncoding.DecodeString(req.Signature)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, verifySignResponse{Error: "signature must be base64-encoded"})
		return
	}

	tx, err := h.Store.BeginReadOnly(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, verifySignResponse{Error: "store error"})
		return
	}
	defer tx.Rollback()

	err = h.ProvenanceSvc.VerifySignature(
		r.Context(),
		tx.SignerEnrollments(),
		caller,
		[]byte(req.Payload),
		sigBytes,
	)
	if err != nil {
		writeJSON(w, http.StatusOK, verifySignResponse{Verified: false, Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, verifySignResponse{Verified: true})
}

func extractBearer(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(auth, prefix) {
		return ""
	}
	return auth[len(prefix):]
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
