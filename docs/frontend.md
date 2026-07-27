# Frontend Components

The frontend is a Bun + TypeScript application served by a Bun/Elysia BFF (Backend-for-Frontend) on port `:3001`. It communicates with the Go backend via the BFF proxy (REST) and a direct WebSocket connection to `:8080/ws`.

---

## Architecture

```
Browser
  ├── SpaceTimeDiagram (D3)
  ├── ClockInspector
  ├── CausalDelivery monitor
  ├── SnapshotViewer
  ├── ConflictDash
  └── ScenarioPanel
        │ REST + WS
  BFF (Bun + Elysia :3001)
        │ REST proxy (forwards Authorization header)
        │ WS passthrough
  Go backend (:8080)
```

The BFF forwards the `Authorization` header on REST requests — required when `VC_API_TOKENS` is configured.

---

## SpaceTimeDiagram

**File**: `frontend/src/components/SpaceTimeDiagram/`

The canonical Lamport 1978 space-time diagram rendered with D3.js.

### What it shows

- **Process lanes**: one vertical axis per process, time flows downward.
- **Events**: dots on each lane — local ticks, sends, receives, markers.
- **Causal arrows**: horizontal or diagonal arcs connecting send events to their corresponding receive events.
- **Concurrent event pairs**: visually distinguished (no causal arrow, same vertical level or out-of-order).
- **Consistent cut overlay**: when a snapshot is finalised, a dashed horizontal line shows the cut through all process lanes.
- **Vector clock tooltip**: hovering an event shows the full vector clock at that point.

### Scrubber

A timeline scrubber lets you step through the history of events. Two modes:

- **Live mode**: scrubber auto-advances to the latest event as they arrive.
- **Replay mode**: scrubber is positioned manually; live events still accumulate but the view is frozen.

!!! note "Live mode bug fix"
    The scrubber's `wasAtEnd` tracking was fixed in the production audit (Issue #103). The scrubber now correctly advances to the latest event when the user was at the end of the timeline before new events arrived.

### Layout modules

| File | Responsibility |
|------|---------------|
| `index.ts` | Entry point — D3 setup, event subscription, render coordination |
| `layout.ts` | Computes x/y positions for each event given the current step |
| `arrows.ts` | Draws causal arc SVG paths (bezier curves) |
| `clocks.ts` | Renders vector clock overlays at each event dot |

---

## ClockInspector

**File**: `frontend/src/components/ClockInspector/index.ts`

Shows the current clock state of each live process.

### What it shows

- Process ID and clock type.
- Full clock value (Lamport integer, vector, or matrix).
- For matrix clocks: full N×N matrix display.
- **Hold-back queue**: messages currently held pending causal delivery, with `BlockedBy` analysis showing which missing messages are blocking each held message.
- Clock divergence indicator: how far ahead/behind each process is relative to others.

### Updates

The inspector subscribes to `clock_tick`, `message_delivered`, and `message_held` WebSocket events and re-renders on each.

---

## CausalDelivery Monitor

**File**: `frontend/src/components/CausalDelivery/index.ts`

Dedicated panel for visualising BSS hold-back queue dynamics.

### What it shows

- Per-process hold-back queue depth (bar chart, live).
- Timeline of `message_held` and `message_delivered` events.
- For each held message: the vector clock delta showing exactly which entry in which process's clock must advance before delivery is possible.
- Average hold time per message.

---

## SnapshotViewer

**File**: `frontend/src/components/SnapshotViewer/index.ts`

Renders completed Chandy-Lamport snapshots.

### What it shows

- List of all completed snapshots (most recent first).
- For the selected snapshot:
  - Each process's captured state (clock at cut time).
  - In-transit messages per channel: count and payloads.
  - Consistent cut line overlaid on the SpaceTimeDiagram.
  - Whether the snapshot is consistent (validated client-side by checking that no recorded receive lacks a recorded send).

---

## ConflictDash

**File**: `frontend/src/components/ConflictDash/index.ts`

Shows the state of the causal KV store and any active conflicts.

### What it shows

- All keys in the store, with their current version count.
- For conflicted keys: side-by-side display of all concurrent versions, their version vectors, and writers.
- Dominance graph: which versions dominate which (visualised as directed arrows between version vector nodes).
- Active resolution strategy and, for non-`keep_all` strategies, the resolved value.

### Interactive

You can write a value from the inspector (calls `PUT /api/v1/kv/:key`) to trigger conflicts live.

---

## ScenarioPanel

**File**: `frontend/src/components/ScenarioPanel/index.ts`

Scenario management UI.

### What it shows

- Cards for each available scenario with a description, estimated duration, and what it demonstrates.
- Run button (calls `POST /api/v1/scenarios/:name/run`).
- Live indicator: which scenario is currently running.
- Result card: duration and outcome once the scenario finishes.
- History: list of previously run scenarios in this session.

---

## TheoryCards

**File**: `frontend/src/components/TheoryCards/index.ts`

Inline reference cards shown alongside the visualisation.

Each card covers one distributed systems concept:

- Happened-before relation
- Vector clock comparison rules
- BSS delivery condition
- Chandy-Lamport consistent cut property
- Version vector conflict detection

Cards are collapsible and auto-highlight when the relevant event type appears in the stream.

---

## State management

The frontend uses [nanostores](https://github.com/nanostores/nanostores) for reactive state:

| Store | Contents |
|-------|---------|
| `processStore` | Live process list with current clocks |
| `eventStore` | All received events (bounded ring buffer, 2000 events) |
| `snapshotStore` | Completed snapshots |
| `kvStore` | Current KV store state |
| `scenarioStore` | Available scenarios + run history |

Components subscribe to stores and re-render on change. All WebSocket events update the stores; components observe only what they need.

---

## BFF proxy

**File**: `frontend/server/bff.ts`

The Bun/Elysia BFF:

- Serves the static frontend bundle on `:3001`.
- Proxies REST calls to `http://localhost:8080` (configurable via `VC_GO_BACKEND`), forwarding `Authorization`, `Content-Type`, and other relevant headers.
- Proxies the WebSocket connection.

```typescript
// Authorization forwarding (required when VC_API_TOKENS is set)
const headers: Record<string, string> = { 'Content-Type': 'application/json' }
const auth = request.headers.get('Authorization')
if (auth) headers['Authorization'] = auth
```

---

## Development

```bash
cd frontend
bun install

# Start BFF + frontend dev server with hot reload
bun run dev

# Build production bundle
bun run build

# Run Playwright browser tests (22 tests)
bunx playwright install chromium
bunx playwright test
```

The Playwright tests use a mocked WebSocket + API setup (`test/playwright/`) and test every component's rendering against known event sequences.
