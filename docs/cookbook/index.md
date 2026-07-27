# Cookbook

Copy-pasteable recipes for the most common Vector Clock Lab tasks. Each recipe is self-contained: it shows the exact API calls, expected events, and what to observe in the UI.

---

## Recipes

| Recipe | What you'll learn |
|--------|------------------|
| [Run your first scenario](basic-scenario.md) | Spawn processes, send messages, observe clocks |
| [Chandy-Lamport snapshot walkthrough](snapshot-walkthrough.md) | Initiate a snapshot and read the consistent cut |
| [Fault injection — delay, drop, partition](fault-injection.md) | Inject network failures and observe recovery |
| [Causal delivery with hold-back queues](causal-delivery.md) | Trigger BSS hold-back and watch the flush |
| [Conflict detection with version vectors](conflict-resolution.md) | Create concurrent writes and resolve conflicts |

---

## Prerequisites

A running backend:

```bash
# Docker Compose (recommended)
docker compose up -d

# Or: run directly
go run ./cmd/server
```

Check it's up:

```bash
curl http://localhost:8080/healthz
# {"status":"ok"}
```

Open the frontend at `http://localhost:3001` to watch events in real time while running the curl commands below.
