package http

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"

	"github.com/coder/websocket"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// SetupEvent is the JSON envelope pushed to WebSocket clients.
type SetupEvent struct {
	Type       string      `json:"type"`
	AuthMethod interface{} `json:"auth_method,omitempty"`
	Error      string      `json:"error,omitempty"`
}

// SetupHub broadcasts setup lifecycle events to connected WebSocket
// clients. Safe for concurrent use.
type SetupHub struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}
	logger  *slog.Logger
}

// NewSetupHub creates a hub ready to accept WebSocket subscribers.
func NewSetupHub(logger *slog.Logger) *SetupHub {
	return &SetupHub{
		clients: make(map[chan []byte]struct{}),
		logger:  logger,
	}
}

// Broadcast serialises ev as JSON and sends it to every connected client.
// Slow or dead clients are dropped.
func (h *SetupHub) Broadcast(ev SetupEvent) {
	data, err := json.Marshal(ev)
	if err != nil {
		h.logger.Error("setup hub: marshal event", "error", err)
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for ch := range h.clients {
		select {
		case ch <- data:
		default:
			// client too slow — drop it
			close(ch)
			delete(h.clients, ch)
		}
	}
}

func (h *SetupHub) subscribe() chan []byte {
	ch := make(chan []byte, 8)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *SetupHub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	if _, ok := h.clients[ch]; ok {
		close(ch)
		delete(h.clients, ch)
	}
	h.mu.Unlock()
}

// AuthMethodCreated satisfies [domain.ProvisionIdPEventSink] so the
// hub can be wired directly as the event sink on
// [domain.ProvisionIdPWorkflowSpec].
func (h *SetupHub) AuthMethodCreated(method domain.AuthMethod) {
	h.Broadcast(SetupEvent{
		Type:       "auth_method_created",
		AuthMethod: authMethodToWS(method),
	})
}

type wsRegistrySubjectMapping struct {
	RegistryID string `json:"registryId"`
	Expression string `json:"expression"`
}

type wsOIDCConfig struct {
	IssuerURL              string                    `json:"issuerUrl"`
	Audience               string                    `json:"audience"`
	AuthorizationEndpoint  string                    `json:"authorizationEndpoint,omitempty"`
	TokenEndpoint          string                    `json:"tokenEndpoint,omitempty"`
	JWKSURI                string                    `json:"jwksUri,omitempty"`
	RegistrySubjectMapping *wsRegistrySubjectMapping `json:"registrySubjectMapping,omitempty"`
}

type wsAuthMethod struct {
	Name       string        `json:"name"`
	Type       string        `json:"type"`
	OIDCConfig *wsOIDCConfig `json:"oidcConfig,omitempty"`
}

func authMethodToWS(m domain.AuthMethod) *wsAuthMethod {
	out := &wsAuthMethod{
		Name: "authMethods/" + string(m.ID),
		Type: "TYPE_OIDC",
	}
	if m.OIDC != nil {
		oidc := &wsOIDCConfig{
			IssuerURL:             string(m.OIDC.IssuerURL),
			Audience:              string(m.OIDC.Audience),
			AuthorizationEndpoint: string(m.OIDC.AuthorizationEndpoint),
			TokenEndpoint:         string(m.OIDC.TokenEndpoint),
			JWKSURI:               string(m.OIDC.JWKSURI),
		}
		if m.OIDC.RegistrySubjectMapping != nil {
			oidc.RegistrySubjectMapping = &wsRegistrySubjectMapping{
				RegistryID: string(m.OIDC.RegistrySubjectMapping.RegistryID),
				Expression: m.OIDC.RegistrySubjectMapping.Expression,
			}
		}
		out.OIDCConfig = oidc
	}
	return out
}

// AuthMethodFailed broadcasts a setup failure event to all WS clients.
func (h *SetupHub) AuthMethodFailed(err error) {
	h.Broadcast(SetupEvent{
		Type:  "auth_method_failed",
		Error: err.Error(),
	})
}

// HandleWS is an http.HandlerFunc that upgrades to WebSocket and
// streams setup events until the client disconnects.
func (h *SetupHub) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // same-origin; no CORS needed
	})
	if err != nil {
		h.logger.Error("setup ws: accept", "error", err)
		return
	}

	ctx := conn.CloseRead(r.Context())
	ch := h.subscribe()
	defer h.unsubscribe(ch)

	for {
		select {
		case <-ctx.Done():
			conn.Close(websocket.StatusNormalClosure, "")
			return
		case msg, ok := <-ch:
			if !ok {
				conn.Close(websocket.StatusNormalClosure, "")
				return
			}
			if err := conn.Write(ctx, websocket.MessageText, msg); err != nil {
				return
			}
		}
	}
}
