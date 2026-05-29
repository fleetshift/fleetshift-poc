package http

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Event is the JSON envelope pushed to WebSocket clients on the
// unified event stream. Clients filter by Type and/or TargetType.
type Event struct {
	Type       string `json:"type"`
	DeliveryID string `json:"deliveryId,omitempty"`
	TargetID   string `json:"targetId,omitempty"`
	TargetType string `json:"targetType,omitempty"`
	EventKind  string `json:"eventKind,omitempty"`
	Message    string `json:"message,omitempty"`
	Timestamp  int64  `json:"timestamp"`
}

// EventHub is a general-purpose WebSocket broadcaster. Any subsystem
// can call [EventHub.Publish] to push events to all connected clients.
// It also implements [domain.DeliveryObserver] to automatically forward
// delivery lifecycle events. Safe for concurrent use.
type EventHub struct {
	domain.NoOpDeliveryObserver

	mu      sync.Mutex
	clients map[chan []byte]struct{}
	logger  *slog.Logger
}

// NewEventHub creates a hub ready to accept WebSocket subscribers.
func NewEventHub(logger *slog.Logger) *EventHub {
	return &EventHub{
		clients: make(map[chan []byte]struct{}),
		logger:  logger,
	}
}

// Publish sends an arbitrary event to all connected clients.
func (h *EventHub) Publish(ev Event) {
	data, err := json.Marshal(ev)
	if err != nil {
		h.logger.Error("event hub: marshal", "error", err)
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for ch := range h.clients {
		select {
		case ch <- data:
		default:
			close(ch)
			delete(h.clients, ch)
		}
	}
}

func (h *EventHub) subscribe() chan []byte {
	ch := make(chan []byte, 16)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *EventHub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	if _, ok := h.clients[ch]; ok {
		close(ch)
		delete(h.clients, ch)
	}
	h.mu.Unlock()
}

// EventEmitted implements [domain.DeliveryObserver].
func (h *EventHub) EventEmitted(ctx context.Context, deliveryID domain.DeliveryID, target domain.TargetInfo, event domain.DeliveryEvent) (context.Context, domain.EventEmittedProbe) {
	h.Publish(Event{
		Type:       "delivery.event",
		DeliveryID: string(deliveryID),
		TargetID:   string(target.ID),
		TargetType: string(target.Type),
		EventKind:  string(event.Kind),
		Message:    event.Message,
		Timestamp:  event.Timestamp.UnixMilli(),
	})
	return ctx, domain.NoOpEventEmittedProbe{}
}

// Completed implements [domain.DeliveryObserver].
func (h *EventHub) Completed(ctx context.Context, deliveryID domain.DeliveryID, target domain.TargetInfo, result domain.DeliveryResult) (context.Context, domain.CompletedProbe) {
	h.Publish(Event{
		Type:       "delivery.completed",
		DeliveryID: string(deliveryID),
		TargetID:   string(target.ID),
		TargetType: string(target.Type),
		EventKind:  string(result.State),
		Message:    result.Message,
		Timestamp:  time.Now().UnixMilli(),
	})
	return ctx, domain.NoOpCompletedProbe{}
}

// HandleWS upgrades to WebSocket and streams events until the client
// disconnects.
func (h *EventHub) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		h.logger.Error("event ws: accept", "error", err)
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
