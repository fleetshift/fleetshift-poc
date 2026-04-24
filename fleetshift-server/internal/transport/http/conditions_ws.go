package http

import (
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"

	transportgrpc "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/grpc"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func NewConditionsWSHandler(broadcaster *transportgrpc.ConditionBroadcaster, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.Warn("websocket upgrade failed", "error", err)
			return
		}
		defer conn.Close()

		id, ch, snapshot := broadcaster.Subscribe()
		defer broadcaster.Unsubscribe(id)

		logger.Debug("conditions websocket client connected", "subscriber_id", id)

		for _, data := range snapshot {
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		}

		// Read pump: discard client messages, detect close
		go func() {
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					broadcaster.Unsubscribe(id)
					return
				}
			}
		}()

		for data := range ch {
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		}
	}
}
