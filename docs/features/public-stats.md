---
title: Public numbers of the site
slug: public-stats
status: beta
epic: E6
owner: pulse
paths: ["internal/public/**", "internal/uptime/**"]
api: ["GET /v1/public-stats"]
events: ["dev.synthwerk.llm.message.completed.v1"]
mcp: []
updated: 2026-11-16
---

# Public numbers of the site

> **In 30 seconds**
> - `GET /v1/public-stats` gives uptime, p95 and answered chats for the public site (synthwerk#47).
> - It needs no token. It holds no personal data and no org ids, only totals of every org.
> - Pulse reads the numbers at most once per minute. The answer has `Cache-Control: public, max-age=60`.
> - The studio site reads it through its own server. The browser never calls pulse.

## What it does

- `uptime_ratio`: the share of target minutes that were not down, over the last 30 UTC days.
- Once per minute, pulse adds one minute per target to `pulse_uptime_daily`. A down target adds no up time.
- Degraded counts as up: the target still answers. A target that pulse did not check yet adds nothing.
- `p95_ms`: the p95 of the edge request time in the last 24 h, from Prometheus.
- The p95 leaves out the `pulse` service. Its health stream stays open up to 1 h.
- `answered_chats`: the completed AI answers of the last 30 UTC days, every org.
- The count comes from the AI usage table of alerts-and-cost.md (column `completed`).
- A number that pulse cannot read is `null`. The other numbers stay.

## How to use

1. Set `SYNTHWERK_PULSE_PROMETHEUS_URL` for the p95.
2. Send `GET /v1/public-stats`. No token.

```json
{"generated_at": "2026-11-16T10:00:00Z", "window_days": 30,
 "uptime_ratio": 0.9993, "p95_ms": 412.5, "answered_chats": 1234}
```

## Rules and limits

| Rule | Value |
|---|---|
| Uptime window | 30 UTC days, today included |
| Uptime sample | 1 min per target. One pulse process writes it. |
| p95 window | 24 h, every edge service but `pulse` |
| Answered chats | Status `completed` only, 30 UTC days |
| Cache | 1 min in pulse. `Cache-Control: public, max-age=60` |
| Uptime rows kept | 400 days |
| Timeout of one refresh | 5 s |

## Events

| Type | When |
|---|---|
| `dev.synthwerk.llm.message.completed.v1` (consumed) | Through the AI usage consumer. A completed answer adds one answered chat. |

## Errors

| Status | Problem type | Cause |
|---|---|---|
| None. | | A number that pulse cannot read is `null`. |

## Tests

- `tests/unit/public/public_test.go` — `many callers inside one minute - sources read once`
- `tests/unit/public/public_test.go` — `no sample, failing sources - null numbers, no error`
- `tests/unit/api/public_test.go` — `public stats - no token, database down - 200, null numbers, public cache`
- `tests/unit/uptime/uptime_test.go` — `Samples - ok and degraded up, down not, unknown left out`
- `tests/unit/metrics/metrics_test.go` — `EdgeP95 - every service but pulse - ms over the window`
- `tests/integration/pulse/public_test.go` — `GET /v1/public-stats - no token - uptime of every target, completed answers of 30 days`

## Changes

| Date | Change |
|---|---|
| 2026-11-16 | First version: uptime, p95 and answered chats without a token (synthwerk#47). |
