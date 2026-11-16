---
title: Alerts, trace links and AI cost per day
slug: alerts-and-cost
status: beta
epic: E8
owner: pulse
paths: ["internal/alert/**", "internal/usage/**"]
api: ["GET /v1/alerts", "GET /v1/metrics", "GET /v1/orgs/{org}/metrics"]
events: ["dev.synthwerk.llm.message.completed.v1"]
mcp: []
updated: 2026-11-16
---

# Alerts, trace links and AI cost per day

> **In 30 seconds**
> - Pulse raises alerts for a down target, for 5xx answers at the edge and for failed AI answers (synthwerk#57).
> - An alert can link the trace of a failed request. Only operators get the link.
> - Pulse counts the AI tokens per day and gives the cost of the last 7 days.
> - The contract is synthwerk-contracts `openapi/pulse/openapi.yaml`.

## What it does

- Pulse checks three rules on every metrics interval (10 s).
- `target_down`: a target of the live status is down. Severity `critical`.
- `service_errors`: the 5xx share of a service at the edge is at or above the threshold. At least one 5xx must be in the window. Severity `warning`.
- `ai_failed`: the share of failed llm answers is at or above the threshold. At least one failed answer must be in the window. Severity `warning`.
- An alert resolves when its rule is false again. A source that pulse cannot read keeps its alerts.
- Alerts live in memory. After a restart, the first check raises the active alerts again.
- Pulse logs `pulse.alert.raised` (WARN) and `pulse.alert.resolved` (INFO).

### Trace links

- `service_errors`: pulse searches Tempo for an edge span with a 5xx answer on `/<service>/...` in the window.
- `ai_failed`: the trace id comes from the `traceparent` of the newest failed `llm.message.completed.v1`.
- `trace_url` is `SYNTHWERK_PULSE_TRACE_URL_TEMPLATE` with `{trace_id}` replaced.
- Traces can hold data of every org. So only operators get `trace_id` and `trace_url`. Other roles get `null`.

### AI cost per day

- A durable consumer `pulse-ai-usage` reads `llm.message.completed.v1` from `EVT_LLM`.
- Each answer counts once. Pulse dedupes on `message_id`.
- Pulse stores answers and tokens per UTC day, org, provider and model alias in `pulse_ai_usage_daily`.
- Every end state counts. A failed answer can also use tokens.
- The cost comes from the price table when pulse reads it. A corrected price also corrects the past days.
- An answer without a price counts in `unpriced_answers` and adds 0 to `cost`.

## How to use

1. Set `SYNTHWERK_PULSE_PROMETHEUS_URL` for `service_errors` (see health-metrics.md).
2. Set `SYNTHWERK_PULSE_TEMPO_URL`, for example `http://telemetry:3200`.
3. Set `SYNTHWERK_PULSE_TRACE_URL_TEMPLATE`. For Grafana in the local stack (data source uid `tempo`):

```text
http://localhost:3000/explore?schemaVersion=1&orgId=1&panes=%7B%22t%22%3A%7B%22datasource%22%3A%22tempo%22%2C%22queries%22%3A%5B%7B%22refId%22%3A%22A%22%2C%22datasource%22%3A%7B%22type%22%3A%22tempo%22%2C%22uid%22%3A%22tempo%22%7D%2C%22queryType%22%3A%22traceql%22%2C%22query%22%3A%22{trace_id}%22%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D
```

4. Set `SYNTHWERK_PULSE_AI_PRICES_FILE` to a price table:

```yaml
currency: USD
prices:
  - provider: anthropic
    model: default        # model alias of the llm event; "*" = every alias
    input_per_million: 3
    output_per_million: 15
  - provider: ollama
    model: "*"
    input_per_million: 0
    output_per_million: 0
```

5. Send `GET /v1/alerts` with an operator, owner or admin token.

`GET /v1/alerts`:

```json
{"generated_at": "2026-11-16T10:00:10Z", "alerts": [
  {"id": "service_errors:llm", "rule": "service_errors", "severity": "warning", "subject": "llm",
   "summary": "3 of 43 llm requests (7.0 %) failed at the edge in the last 5 min.",
   "value": 0.0698, "threshold": 0.05, "since": "2026-11-16T09:58:40Z",
   "trace_id": "d40c39905663a1bcaa36ad07693a23d8", "trace_url": "http://localhost:3000/explore?..."}]}
```

`ai_cost` in `GET /v1/metrics` (every org) and `GET /v1/orgs/{org}/metrics` (that org):

```json
{"currency": "USD", "days": [
  {"date": "2026-11-16", "cost": 0.4215, "answers": 120, "input_tokens": 48000, "output_tokens": 9100, "unpriced_answers": 2}]}
```

## Rules and limits

| Rule | Value |
|---|---|
| Threshold of `service_errors` and `ai_failed` | 0.05 (`SYNTHWERK_PULSE_ALERT_ERROR_RATIO`, above 0 to 1) |
| Window | Same as the metrics window, 5 min (`SYNTHWERK_PULSE_METRICS_WINDOW`) |
| Edge counts | Counter now minus counter one window back. The first 5xx of a new series counts. |
| Trace search | At most once per 30 s per alert without a trace |
| Alerts in the answer | Max 100. Critical first, then oldest first. |
| Days in `ai_cost` | Always 7 UTC days, oldest first, today last |
| Default price table | USD. `ollama` and `fake` cost 0. Every other provider is unpriced. |
| Dedupe window | 32 days. An older answer does not count. Day rows stay 400 days. |
| Price file | Max 1 MiB, 256 prices, 0 to 10,000 per million tokens |

## Events

| Type | When |
|---|---|
| `dev.synthwerk.llm.message.completed.v1` (consumed) | The llm service finished an answer. Pulse counts it for the cost and checks `ai_failed`. |

Pulse sends no alert event yet. A later notification service can add one in synthwerk-contracts.

## Errors

| Status | Problem type | Cause |
|---|---|---|
| 401 | `about:blank`, code `unauthenticated` | No Bearer token, or the token is not valid. |
| 403 | `about:blank`, code `forbidden` | The token has no `operator`, `owner` or `admin` role, for example a member. |

## Tests

- `tests/unit/alert/alert_test.go` — `forced 5xx at the edge - service_errors alert with a trace link that opens the trace`
- `tests/unit/alert/alert_test.go` — `caller is not an operator - no trace fields`
- `tests/unit/alert/alert_test.go` — `failed answers - ai_failed alert with the trace of the newest failed answer`
- `tests/unit/alert/alert_test.go` — `target down - critical alert first, since of the status`
- `tests/unit/metrics/metrics_test.go` — `EdgeCounts - new 5xx series - counted from zero, errors at most requests`
- `tests/unit/usage/usage_test.go` — `today - priced cost, local answers free, unpriced counted`
- `tests/unit/usage/usage_test.go` — `answers - each counted once, a failed record is delivered again`
- `tests/integration/pulse/alerts_test.go` — `forced error - ai_failed alert with the trace link of the failed answer`
- `tests/integration/pulse/alerts_test.go` — `AI cost - real Postgres - each answer once, priced per org`

## Changes

| Date | Change |
|---|---|
| 2026-11-16 | First version: alerts, trace links and AI cost per day (synthwerk#57). |
