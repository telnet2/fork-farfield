# Farfield Architecture

Farfield is a web-based remote control and monitoring dashboard for AI coding agents. It supports [Codex](https://openai.com/codex) and [OpenCode](https://opencode.ai), providing a unified interface to browse conversations, send messages, switch models, and monitor agent activity.

## Monorepo Layout

```
farfield/
├── apps/
│   ├── server/              # Node.js HTTP server (the bridge between web UI and agents)
│   └── web/                 # React frontend (Vite SPA)
├── packages/
│   ├── codex-protocol/      # Zod schemas and types for Codex wire formats
│   ├── codex-api/           # Typed clients for Codex (app-server + desktop IPC)
│   └── opencode-api/        # Typed client for OpenCode (REST via official SDK)
└── scripts/                 # Build, dev, and schema generation helpers
```

## Component Overview

### Web UI (`apps/web`)

React SPA served by Vite. Communicates with the Farfield server over:

- **REST** for actions (list threads, send messages, create threads, etc.)
- **SSE** (`/events`) for real-time state updates and IPC history

### Server (`apps/server`)

Node.js HTTP server that acts as a unified gateway. Exposes a single REST+SSE API and routes requests to the correct backend through an `AgentRegistry` containing pluggable `AgentAdapter` implementations.

Key server-side pieces:

- `AgentRegistry` — manages registered adapters, resolves which adapter handles a given thread
- `ThreadIndex` — maps thread IDs to their owning agent
- `CodexAgentAdapter` — Codex integration (app-server + desktop IPC)
- `OpenCodeAgentAdapter` — OpenCode integration (REST SDK)

### Protocol Package (`packages/codex-protocol`)

Zod-based schema validation for all Codex wire formats: IPC frames, thread stream events, conversation state, and app-server responses. Schemas use `.passthrough()` to tolerate unknown fields while strictly validating known ones. Parse helpers throw `ProtocolValidationError` with detailed issue paths.

App-server schemas are vendored from the official Codex schema output and auto-generated into Zod modules via `bun run generate:codex-schema`.

### Codex API Package (`packages/codex-api`)

Typed client layer for Codex with three main pieces:

- `AppServerClient` — sends JSON-RPC requests to the Codex app-server subprocess
- `DesktopIpcClient` — connects to the Codex desktop IPC socket for real-time operations
- `CodexMonitorService` — high-level actions (send message, set mode, interrupt, submit user input)

### OpenCode API Package (`packages/opencode-api`)

Wraps the official `@opencode-ai/sdk` package. Provides:

- `OpenCodeConnection` — manages the SDK client lifecycle
- `OpenCodeMonitorService` — session and message operations
- Mapper functions that translate OpenCode data shapes into Farfield's unified thread format

## Protocols

Farfield uses **different protocols for each backend**. Here is a summary:

| Backend   | Protocol                            | Transport                          | Pattern                                  |
|-----------|-------------------------------------|------------------------------------|------------------------------------------|
| Codex     | JSON-RPC 2.0 (app-server)          | stdio (child process)              | Request/Response                         |
| Codex     | Custom IPC (length-prefixed JSON)   | Unix domain socket                 | Bidirectional (request/response + broadcast) |
| OpenCode  | REST HTTP                           | HTTP client (`@opencode-ai/sdk`)   | Request/Response                         |
| Web UI    | REST + SSE                          | HTTP                               | REST for actions, SSE for live updates   |

### Codex App-Server Protocol

The Codex app-server protocol is **JSON-RPC 2.0 over stdin/stdout**. Farfield spawns the Codex CLI as a child process:

```
codex app-server
```

and communicates via newline-delimited JSON-RPC over the subprocess stdio pipes (`packages/codex-api/src/app-server-transport.ts`).

**Initialization handshake:**

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "clientInfo": { "name": "farfield", "version": "0.2.0" },
    "capabilities": { "experimentalApi": true }
  }
}
```

**Methods used:**

| Method                    | Purpose                          |
|---------------------------|----------------------------------|
| `initialize`              | Handshake with client info       |
| `thread/list`             | List threads (paginated)         |
| `thread/read`             | Read thread with turns           |
| `thread/start`            | Create a new thread              |
| `thread/resume`           | Resume an archived thread        |
| `model/list`              | List available models            |
| `collaborationMode/list`  | List collaboration modes         |
| `sendUserMessage`         | Send a message to a thread       |

### Codex Desktop IPC Protocol

For real-time operations on active threads (sending messages, interrupting, setting collaboration modes), Farfield connects to the **Codex desktop IPC socket** (`packages/codex-api/src/ipc-client.ts`).

**Socket path:** `/tmp/codex-ipc/ipc-<uid>.sock` (configurable via `CODEX_IPC_SOCKET`)

**Wire format:** 4-byte little-endian length prefix followed by a UTF-8 JSON payload.

```
[4 bytes: UInt32LE payload size][N bytes: UTF-8 JSON]
```

**Frame types:**

| Frame Type                    | Direction | Purpose                                      |
|-------------------------------|-----------|----------------------------------------------|
| `request`                     | Out       | Send an RPC-style request with a requestId   |
| `response`                    | In        | Response to a request (success or error)      |
| `broadcast`                   | Both      | Fire-and-forget notification                  |
| `client-discovery-request`    | In        | Codex asks if client can handle a request     |
| `client-discovery-response`   | Out       | Client replies (Farfield always says no)      |

**IPC methods used by Farfield:**

| Method                                  | Purpose                                    |
|-----------------------------------------|--------------------------------------------|
| `initialize`                            | Register as an IPC client, receive clientId |
| `thread-follower-start-turn`            | Send a message to an active thread          |
| `thread-follower-set-collaboration-mode`| Change the collaboration mode               |
| `thread-follower-submit-user-input`     | Respond to a tool approval prompt           |
| `thread-follower-interrupt-turn`        | Interrupt the current turn                  |

**Live state updates:** Codex broadcasts `thread-stream-state-changed` events over IPC containing snapshots and patches of conversation state. Farfield reduces these into a materialized live thread state using `reduceThreadStreamEvents`.

**Message flow example (send message via IPC):**

```
1. Farfield → IPC: initialize request
2. Codex   → IPC: response with clientId
3. Farfield → IPC: thread-follower-start-turn request
   {conversationId, turnStartParams: {threadId, input: [{type: "text", text}], ...}, isSteering: false}
   targetClientId = thread owner's clientId
4. Codex   → IPC: response (success)
```

### OpenCode Protocol

OpenCode uses a **REST HTTP API** accessed through the official `@opencode-ai/sdk` package (`packages/opencode-api/src/client.ts`). Farfield either connects to an existing server URL or spawns one via `createOpencode()`.

**SDK methods used:**

| SDK Call                   | Purpose                        |
|----------------------------|--------------------------------|
| `client.session.list()`    | List sessions (optionally by directory) |
| `client.session.create()`  | Create a new session           |
| `client.session.get()`     | Get session details            |
| `client.session.messages()` | Get messages for a session    |
| `client.session.prompt()`  | Send a message                 |
| `client.session.abort()`   | Interrupt the active turn      |
| `client.project.list()`    | List known project directories |

OpenCode's session/message data model is mapped to Farfield's unified thread format via `packages/opencode-api/src/mapper.ts`.

### Farfield HTTP API (Server to Web UI)

The server exposes a REST API on port 4311 (default) plus an SSE endpoint:

**Core endpoints:**

| Method | Path                                    | Purpose                        |
|--------|-----------------------------------------|--------------------------------|
| GET    | `/api/agents`                           | List agents and capabilities   |
| GET    | `/api/threads`                          | List threads from all agents   |
| POST   | `/api/threads`                          | Create a thread                |
| GET    | `/api/threads/:id`                      | Read a thread                  |
| GET    | `/api/threads/:id/live-state`           | Get live conversation state    |
| GET    | `/api/threads/:id/stream-events`        | Get raw IPC stream events      |
| POST   | `/api/threads/:id/messages`             | Send a message                 |
| POST   | `/api/threads/:id/collaboration-mode`   | Set collaboration mode         |
| POST   | `/api/threads/:id/user-input`           | Submit user input response     |
| POST   | `/api/threads/:id/interrupt`            | Interrupt active turn          |
| GET    | `/api/models`                           | List available models          |
| GET    | `/api/collaboration-modes`              | List collaboration modes       |
| GET    | `/events`                               | SSE stream for live updates    |

**SSE event types:**

- `{type: "state", state: {...}}` — runtime state snapshot
- `{type: "history", entry: {...}}` — new IPC/action history entry

## Data Flow

### Sending a Message

```
Web UI                  Farfield Server              Agent Backend
  │                          │                            │
  │ POST /threads/:id/msg    │                            │
  │─────────────────────────>│                            │
  │                          │ resolve adapter from        │
  │                          │ thread index                │
  │                          │                            │
  │                          │──── Codex IPC request ────>│  (if Codex + IPC ready)
  │                          │    or                       │
  │                          │──── app-server RPC ───────>│  (if Codex, no IPC)
  │                          │    or                       │
  │                          │──── HTTP SDK call ────────>│  (if OpenCode)
  │                          │                            │
  │                          │<─── response ──────────────│
  │<── {ok: true} ──────────│                            │
  │                          │                            │
  │                          │<── IPC broadcast ──────────│  (thread-stream-state-changed)
  │<── SSE event ────────────│                            │
```

### Live State Updates (Codex only)

```
Codex Desktop            Farfield Server              Web UI
  │                          │                            │
  │── broadcast ────────────>│                            │
  │   thread-stream-state-   │ reduce snapshots+patches   │
  │   changed                │ into conversation state    │
  │                          │                            │
  │                          │──── SSE event ────────────>│
  │                          │                            │
  │                          │     GET /live-state        │
  │                          │<───────────────────────────│
  │                          │──── materialized state ───>│
```

## Capability Differences

Not all features are available for both agents:

| Capability               | Codex | OpenCode |
|--------------------------|-------|----------|
| List threads             | Yes   | Yes      |
| Create threads           | Yes   | Yes      |
| Read thread / turns      | Yes   | Yes      |
| Send messages            | Yes   | Yes      |
| Interrupt                | Yes   | Yes      |
| List models              | Yes   | No       |
| List collaboration modes | Yes   | No       |
| Set collaboration mode   | Yes   | No       |
| Submit user input        | Yes   | No       |
| Live state (IPC stream)  | Yes   | No       |
| Raw stream events        | Yes   | No       |
