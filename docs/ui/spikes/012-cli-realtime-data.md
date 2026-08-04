# SPIKE: CLI Real-Time Data

**Jira**: FM-TBD (pending approval to create)
**Epic**: SPIKE - User Interface (CLI)
**Status**: Done
**Related**: [006 - Live Cluster Data and Real-Time Updates](006-live-data-and-websockets.md)

## Problem

The CLI currently fetches data via REST — run a command, get a snapshot, done. There are no live updates. If a pod crashes after the user ran `pods prod-cluster`, they will not know until they run the command again.

## What was explored

Most of the real-time data architecture is covered in spike 006 — K8s informers, WebSocket delivery, topic-based pub/sub, and the event bus pattern. The same backend serves both the GUI and the CLI, so the server-side infrastructure is already in place.

WebSockets work in Node.js. Unlike in the browser, the Node.js WebSocket client supports custom headers, so authentication can be handled with a standard `Authorization` header on the connection upgrade request. The ticket-based handshake used in the GUI (spike 006) is a workaround for a browser limitation that does not apply here.

The CLI could subscribe to the same topics as the GUI and update plugin stores the same way. In fullscreen TUI mode, the display can be refreshed in place as events arrive. In scrolling mode, new events could be appended to the output. In single command mode, live updates do not apply — the command runs once and exits.

This is not implemented in the POC. The plumbing from spike 006 covers the protocol and server side. The CLI-specific work is connecting to the WebSocket, subscribing to topics, and deciding how each rendering mode handles incoming updates.

### Open questions

- Should the CLI support a `--watch` flag that keeps a command running and refreshes as data changes?
- How do we handle high event volumes in the terminal without flooding the output?
- Should live updates be opt-in per command or always-on in interactive modes?

## Key files

- GUI event bus (reference implementation): `packages/gui/src/hooks/useInvalidationSocket.ts`
- WebSocket server: `packages/mock-servers/src/ws.ts`
- CLI fullscreen frame: `packages/cli/src/components/FullScreenFrame.tsx`
- CLI scrolling frame: `packages/cli/src/components/ScrollingFrame.tsx`
