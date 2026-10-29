---
title: Health metrics and access per role
slug: health-metrics
status: beta
epic: E4
owner: pulse
paths: ["internal/metrics/**", "internal/app/**", "cmd/pulse/**"]
api: ["GET /v1/metrics", "GET /v1/orgs/{org}/metrics", "GET /v1/health", "GET /v1/health/stream"]
events: ["dev.synthwerk.llm.message.completed.v1"]
mcp: []
updated: 2026-10-30
---

# Health metrics and access per role

> **In 30 seconds**
> - Pulse gives the numbers of the studio health view (synthwerk#40).
> - Every `/v1` route needs a Bearer token. Members get 403.
> - Operators read the platform numbers. Org owners and admins read the AI numbers of their own org only.
> - A part that pulse cannot read is `null`. The other parts stay.

## What it does

- Rate, error share and p95 per service come from the edge metrics in Prometheus.
- Host CPU, memory and disk come from `/proc` and the root file system.
- Database connections come from `pg_stat_activity`. Pending messages come from the JetStream consumers.
- Tokens per minute and model latency come from `llm.message.completed.v1` on `EVT_LLM`.
- Pulse reads the platform numbers once per interval. The load does not grow with the number of viewers.
- Pulse reads the llm events live. At start it reads one window back.
- The AI numbers are kept per org, from the `orgid` of each event.

## Access per role

| Route | operator | owner, admin | member | no token |
|---|---|---|---|---|
| `GET /v1/health`, `GET /v1/health/stream` | 200 | 200 | 403 | 401 |
| `GET /v1/metrics` (every org) | 200 | 403 | 403 | 401 |
| `GET /v1/orgs/{org}/metrics`, own org | 200 | 200 | 403 | 401 |
| `GET /v1/orgs/{org}/metrics`, other org | 200 | 403 | 403 | 401 |

- `operator` counts only when the grant is in `SYNTHWERK_PULSE_PLATFORM_ORG_ID`.
- An owner or admin grant counts only in the org of the token.
- The status of platform parts holds no org data. Org admins may read it.

## How to use

1. Set `SYNTHWERK_PULSE_OIDC_ISSUER`. Pulse does not start without it.
2. Set `SYNTHWERK_PULSE_PLATFORM_ORG_ID` to the Zitadel org that grants `operator`.
3. Set `SYNTHWERK_PULSE_PROMETHEUS_URL` for the service numbers, for example `http://telemetry:9090`.
4. Send `GET /v1/metrics` with an operator token. Send `GET /v1/orgs/<org id>/metrics` with an org admin token.

`GET /v1/metrics`:

```json
{
  "generated_at": "2026-10-30T08:00:10Z",
  "window_seconds": 300,
  "services": [{"service": "llm", "requests_per_second": 2, "error_ratio": 0.01, "p95_ms": 250}],
  "host": {"cpu_ratio": 0.12, "memory_used_bytes": 4100000000, "memory_total_bytes": 16000000000,
           "disk_used_bytes": 51000000000, "disk_total_bytes": 250000000000},
  "database": {"connections": 14, "max_connections": 100},
  "bus": {"pending_messages": 3, "streams": [{"stream": "EVT_LLM", "messages": 120, "pending": 3}]},
  "ai": {"tokens_per_minute": 840.5, "answers": 12, "failed": 1, "latency_p95_ms": 2300}
}
```

`GET /v1/orgs/{org}/metrics`:

```json
{"generated_at": "2026-10-30T08:00:12Z", "window_seconds": 300, "org_id": "339201",
 "ai": {"tokens_per_minute": 120, "answers": 3, "failed": 0, "latency_p95_ms": 900}}
```

| Field | Meaning |
|---|---|
| `services` | Per edge service: requests per second, share of `5xx` answers, p95 in ms. `p95_ms` is `null` without traffic. |
| `host.cpu_ratio` | Busy share of all CPUs since the last refresh, 0 to 1. `null` on the first refresh. |
| `bus.pending` | Per stream: not delivered plus not acked, summed over the consumers. |
| `ai` | Every answer in the window. `latency_p95_ms` covers completed answers only. |

## Rules and limits

| Rule | Value |
|---|---|
| Window of rates, p95 and AI numbers | 5 min (`SYNTHWERK_PULSE_METRICS_WINDOW`, 1 min to 1 h) |
| Refresh of the platform numbers | 10 s (`SYNTHWERK_PULSE_METRICS_INTERVAL`, 1 s to 5 min) |
| Timeout per part | The refresh interval, at most 5 s |
| AI samples kept | Max 50,000. The oldest go first. |
| Audience | `SYNTHWERK_PULSE_OIDC_AUDIENCE`, default `synthwerk-api` |
| Host numbers | `SYNTHWERK_PULSE_PROC_DIR` (default `/proc`), `SYNTHWERK_PULSE_DISK_PATH` (default `/`) |
| Edge metric names | `traefik_service_requests_total`, `traefik_service_request_duration_seconds_bucket` |
| Readiness | `jwks` is a non-critical check: no keys gives `degraded`, not `down`. |

## Events

| Type | When |
|---|---|
| `dev.synthwerk.llm.message.completed.v1` (consumed) | The llm service finished an answer. Pulse adds its tokens and latency to the window. |

## Errors

| Status | Problem type | Cause |
|---|---|---|
| 401 | `about:blank`, code `unauthenticated` | No Bearer token, or the token is not valid. |
| 403 | `about:blank`, code `forbidden` | The role does not allow the route or the org. |
| 503 | `about:blank`, code `auth_unavailable` | Pulse cannot load the token keys. |

## Tests

- `tests/unit/api/api_test.go` — `org metrics - admin of another org - 403`
- `tests/unit/api/api_test.go` — `org metrics - org admin - numbers of the own org only`
- `tests/unit/api/api_test.go` — `platform metrics - operator grant outside the platform org - 403`
- `tests/unit/metrics/metrics_test.go` — `Stats - one org - only the answers of that org`
- `tests/unit/metrics/metrics_test.go` — `Sample - two samples - CPU share between them, memory and disk`
- `tests/unit/metrics/metrics_test.go` — `Services - edge metrics - rate, error share and p95 per service`
- `tests/integration/pulse/pulse_test.go` — `AI - events on EVT_LLM - tokens and latency per org`
- `tests/integration/pulse/pulse_test.go` — `Platform - real Postgres and NATS - connections and pending messages`

## Changes

| Date | Change |
|---|---|
| 2026-10-30 | First version: token check on every `/v1` route, platform and org metrics (synthwerk#40). |
