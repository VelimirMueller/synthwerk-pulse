---
title: Live status per service
slug: live-status
status: beta
epic: E1
owner: pulse
paths: ["cmd/pulse/**", "internal/**", "targets.example.yaml"]
api: ["GET /v1/health", "GET /v1/health/stream", "GET /healthz", "GET /readyz"]
events: ["dev.synthwerk.pulse.target.status_changed.v1"]
mcp: []
updated: 2026-10-30
---

# Live status per service

> **In 30 seconds**
> - Pulse checks every service and infra part from `targets.yaml` and keeps a stable status for each.
> - `GET /v1/health/stream` sends the status as Server-Sent Events: a snapshot first, then one event per change.
> - A stopped part shows as `down` in about 10 to 15 s. The design limit is 30 s.
> - Each stored change goes through the outbox as `dev.synthwerk.pulse.target.status_changed.v1`.

## What it does

- Pulse reads its targets from the file in `SYNTHWERK_PULSE_TARGETS_FILE` at start. No API adds a target.
- Kind `http` sends `GET` to the URL. A `/readyz` body gives the status and the checks.
- Any other `2xx` or `3xx` answer is `ok`. A `4xx`, a `5xx`, a timeout or a refused connection is `down`.
- Kind `tcp` opens a TCP connection. It is for parts without a health endpoint, such as Postgres.
- Pulse follows no redirect and sends no cookie. It opens a new connection for each check.
- A status changes only after a run of equal results (table below). One lost check does not turn a tile red.
- Every change goes to the open streams at once, also when Postgres is down.
- Pulse stores the last status per target in `pulse_target_status`. In the same transaction it writes the event to the outbox.
- When Postgres is down, the stored status catches up on the next check after Postgres is back.
- A restart does not send an event again: pulse writes an event only when the stored status differs.

| From | To | After |
|---|---|---|
| `unknown` | `ok` or `degraded` | 1 result that is not `down` |
| any | `down` | 2 `down` results in a row |
| `ok` | `degraded` | 1 `degraded` result |
| `degraded` | `ok` | 2 `ok` results in a row |
| `down` | `ok` or `degraded` | 3 results in a row that are not `down` |

## How to use

1. Write a targets file. Start from `targets.example.yaml`.
2. Set `SYNTHWERK_PULSE_TARGETS_FILE`, `SYNTHWERK_PULSE_DATABASE_URL` and `SYNTHWERK_PULSE_NATS_URL`.
3. Start pulse. The synthwerk-infra compose stack does this for you.
4. Open the stream with a token of an operator, owner or admin: `curl -N -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/pulse/v1/health/stream` (through the local edge).
5. Read the first `snapshot` event for the full state. Then apply each `status` event.

Stream format (`text/event-stream`):

```
retry: 3000

id: 0
event: snapshot
data: {"targets":[{"target":"bus","status":"ok","since":"2026-10-21T08:00:05Z","checked_at":"2026-10-21T08:02:10Z","latency_ms":2,"checks":[]}]}

id: 1
event: status
data: {"target":"bus","from":"ok","status":"down","since":"2026-10-21T08:02:15Z","checks":[]}

: ping
```

| Event | Data |
|---|---|
| `snapshot` | `targets`: list of `target`, `status`, `since`, `checked_at` (null before the first check), `latency_ms`, `checks` |
| `status` | `target`, `from`, `status`, `since`, `checks` |
| `: ping` | Comment line every 15 s. No data. |

- `status` is `unknown`, `ok`, `degraded` or `down`. `checks` lists `name` and `status` from the target's `/readyz` body.
- The `id` grows by one per change. It starts at 0 in each pulse process.
- Every stream starts with a snapshot, also after a reconnect with `Last-Event-ID`.
- `GET /v1/health` returns the snapshot data as one JSON document.
- The stream shape is defined here for M1. It moves to synthwerk-contracts when the studio consumes it.

## Rules and limits

| Rule | Value |
|---|---|
| Default check interval | 15 s (`SYNTHWERK_PULSE_DEFAULT_INTERVAL`); infra targets in compose: 5 s |
| Default check timeout | 3 s (`SYNTHWERK_PULSE_DEFAULT_TIMEOUT`) |
| Interval range | 1 s to 5 min. Timeout at most the interval and at most 10 s. |
| Targets | Max 64. Name `^[a-z][a-z0-9-]{0,63}$`. URL `http` or `https`, no user or password. |
| `/readyz` body read | Max 64 KiB. Max 32 checks kept. |
| Open streams | Max 50 per process (`SYNTHWERK_PULSE_SSE_MAX_CLIENTS`). Then 503 with `Retry-After: 5`. |
| Stream lifetime | 1 h. Then the stream ends and the client reconnects. |
| Slow client | More than 64 events behind: pulse closes the stream. |
| Own readiness | `postgres` and `nats` are critical. |

## Events

| Type | When |
|---|---|
| `dev.synthwerk.pulse.target.status_changed.v1` | The stored status of a target changes. `from` is the stored status, `unknown` the first time. |

## Errors

| Status | Problem type | Cause |
|---|---|---|
| 401 | `about:blank`, code `unauthenticated` | No Bearer token, or the token is not valid. See [health-metrics.md](health-metrics.md). |
| 403 | `about:blank`, code `forbidden` | The token has no `operator`, `owner` or `admin` role. |
| 503 | none (plain text) | `/v1/health/stream`: the max number of streams is open, or pulse is shutting down. |
| 503 | none (body is `Readiness`) | `/readyz`: startup is not complete, or Postgres or NATS is down. |

## Not in M1

- One pulse process only: no leader lock and no fan-out over `sys.health.>` yet.
- No incidents, alerts or KPIs. No audit consumer yet: the audit event names are still open. Metrics: [health-metrics.md](health-metrics.md).
- Pulse uses the shared database with `pulse_` tables. Its own database and role come with the infra module.

## Tests

- `tests/unit/status/status_test.go` — `Observe - ok and 2 consecutive failures - down`
- `tests/unit/status/status_test.go` — `Observe - down then 2 ok - still down`
- `tests/unit/check/check_test.go` — `Check - readyz 503 down - down with checks`
- `tests/unit/check/check_test.go` — `Check - target hangs - down within the timeout`
- `tests/unit/target/target_test.go` — `Parse - url with password - error`
- `tests/unit/stream/stream_test.go` — `stream - connect - snapshot with every target first`
- `tests/unit/stream/stream_test.go` — `stream - max clients reached - 503 with Retry-After`
- `tests/unit/monitor/monitor_test.go` — `CheckOnce - database down - stream still changes, store catches up later`
- `tests/integration/pulse/pulse_test.go` — `stream - target stops - down within 3 intervals`
- `tests/integration/pulse/pulse_test.go` — `outbox - every change - one valid event each on EVT_PULSE`
- `tests/integration/pulse/pulse_test.go` — `Save - same status again, also from a new store - no event`
- synthwerk-infra `tests/stack/stack_test.sh` — stop a container, the stream says `down`; start it, the stream says `ok`.

## Changes

| Date | Change |
|---|---|
| 2026-10-21 | First version: targets file, http and tcp checks, hysteresis, SSE stream, status events through the outbox. |
| 2026-10-30 | `/v1/health` and the stream need a token of an operator, owner or admin (synthwerk#40). |
