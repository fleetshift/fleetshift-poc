# Live Data: Retired UI Prototype

The former mock-server informer, topic subscription, log-streaming, and WebSocket-ticket prototype is no longer implemented. Its protocol is not a production contract.

Current UI resource queries are described in [API access](../api-requirements.md). The shell's event subscription API remains a stub in [ScalprumShell](../../../client/web/src/components/Root/ScalprumShell.tsx). The separate Go [delivery-event WebSocket](../../../server/internal/transport/http/event_ws.go) does not implement that removed resource-subscription protocol.

This page preserves existing historical links without retaining obsolete setup instructions or proposed protocol details.
