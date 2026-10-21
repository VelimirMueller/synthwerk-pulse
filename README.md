<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/banner/hero-v2-light.svg">
  <img alt="SYNTHWERK-PULSE. The platform watches itself. Every box gets a live tile. Draft, M1." src="assets/banner/hero-v2-dark.svg" width="100%">
</picture>
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/lead-v2-light.svg">
  <img alt="synthwerk-pulse is the platform&#x27;s view of itself. It checks every service and infra part from targets.yaml and streams a stable status per target over Server-Sent Events." src="assets/readme/lead-v2-dark.svg" width="100%">
</picture>
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/stats-v2-light.svg">
  <img alt="2 CHECK KINDS: HTTP AND TCP. 30 s LIMIT TO SHOW A STOPPED PART. 50 OPEN STREAMS PER PROCESS. 1 EVENT PER STORED CHANGE" src="assets/readme/stats-v2-dark.svg" width="100%">
</picture>
</p>

<br>

<p>
<img alt="01 WHAT IT DOES. ONE TILE PER TARGET." src="assets/readme/divider-what-v2.svg" width="100%">
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/features-v2-light.svg">
  <img alt="CHECK: Kind http sends GET to a URL. Kind tcp connects to host:port. One target per box. HOLD: A status changes only after a run of equal results. One lost check stays green. STREAM: Server-Sent Events: a snapshot first, then one event per change. A ping every 15 s" src="assets/readme/features-v2-dark.svg" width="100%">
</picture>
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/what-v2-light.svg">
  <img alt="Targets file: Pulse reads SYNTHWERK_PULSE_TARGETS_FILE at start. No API adds a target. Max 64 targets. http: A /readyz body from openapi/_shared/health.yaml gives status and checks. Other 2xx or 3xx is ok. down: A 4xx, a 5xx, a timeout or a refused connection is down. Pulse follows no redirect. tcp: Opens a TCP connection to host:port. For parts without a health endpoint, such as Postgres. Stored: The last status per target sits in pulse_target_status. The event goes to the outbox in the same transaction. Fast: Compose infra targets poll every 5 s. A stopped part shows as down in about 10 to 15 s" src="assets/readme/what-v2-dark.svg" width="100%">
</picture>
</p>

<br>

<p>
<img alt="02 QUICK START. UP. CURL. WATCH." src="assets/readme/divider-start-v2.svg" width="100%">
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/start-v2-light.svg">
  <img alt="Terminal: # in synthwerk-infra | $ scripts/init-env.sh &amp;&amp; docker compose up --wait | $ curl -N http://127.0.0.1:8080/pulse/v1/health/stream | event: snapshot | # stop a container: a status event says down" src="assets/readme/start-v2-dark.svg" width="100%">
</picture>
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/setup-v2-light.svg">
  <img alt="just test-unit: Status, check, target, stream and monitor tests in tests/unit. No Docker. just test-int: Store, outbox and live status against real Postgres and NATS. Needs Docker. just image: Build the container image as CI builds it. just lint: gofmt, golangci-lint, actionlint and zizmor" src="assets/readme/setup-v2-dark.svg" width="100%">
</picture>
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/startnotes-v2-light.svg">
  <img alt="Needs: Go 1.27. Docker for just test-int and just image. just runs the same commands as CI. Own targets: Start from targets.example.yaml. Set the targets file, database URL and NATS URL" src="assets/readme/startnotes-v2-dark.svg" width="100%">
</picture>
</p>

<details>
<summary><picture><source media="(prefers-color-scheme: light)" srcset="assets/readme/links-7-v2-light.svg"><img alt="Plain text" src="assets/readme/links-7-v2-dark.svg" width="32%"></picture></summary>

```bash
# in synthwerk-infra
scripts/init-env.sh && docker compose up --wait
curl -N http://127.0.0.1:8080/pulse/v1/health/stream

# in synthwerk-pulse
just test-unit    # unit tests, no Docker
just test-int     # Postgres + NATS, needs Docker
just image        # container image as CI builds it
```

</details>

<p align="center">
<a href="docs/features/live-status.md"><picture><source media="(prefers-color-scheme: light)" srcset="assets/readme/links-1-v2-light.svg"><img alt="Feature doc (docs/features/live-status.md)" src="assets/readme/links-1-v2-dark.svg" width="32%"></picture></a>
<a href="targets.example.yaml"><picture><source media="(prefers-color-scheme: light)" srcset="assets/readme/links-2-v2-light.svg"><img alt="Targets (targets.example.yaml)" src="assets/readme/links-2-v2-dark.svg" width="32%"></picture></a>
<a href="https://github.com/VelimirMueller/synthwerk/issues/26"><picture><source media="(prefers-color-scheme: light)" srcset="assets/readme/links-3-v2-light.svg"><img alt="Issue (https://github.com/VelimirMueller/synthwerk/issues/26)" src="assets/readme/links-3-v2-dark.svg" width="32%"></picture></a>
</p>

<br>

<p>
<img alt="03 HOW IT WORKS. ONE LOST CHECK IS NOT RED." src="assets/readme/divider-how-v2.svg" width="100%">
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/flow-v2-light.svg">
  <img alt="TARGET -&gt; CHECK -&gt; HOLD -&gt; STREAM. A change goes to the open streams at once, also when Postgres is down." src="assets/readme/flow-v2-dark.svg" width="100%">
</picture>
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/how-v2-light.svg">
  <img alt="To down: After 2 down results in a row, from any status. Back up: From down to ok or degraded after 3 results in a row that are not down. Degraded: From ok to degraded at once. Back to ok after 2 ok results in a row. First: From unknown to ok or degraded after 1 result that is not down. Restart: No event again: pulse writes an event only when the stored status differs" src="assets/readme/how-v2-dark.svg" width="100%">
</picture>
</p>

<br>

<p>
<img alt="04 USAGE. ROUTES, EVENTS, KNOBS." src="assets/readme/divider-usage-v2.svg" width="100%">
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/api-v2-light.svg">
  <img alt="GET /healthz: Liveness of the process. GET /readyz: Readiness. postgres and nats are critical. GET /v1/health: The snapshot as one JSON document. GET /v1/health/stream: SSE: snapshot first, then status per change, : ping every 15 s. Max 50 streams, then 503. 1 h lifetime. local edge: http://127.0.0.1:8080/pulse/v1/health/stream" src="assets/readme/api-v2-dark.svg" width="100%">
</picture>
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/events-v2-light.svg">
  <img alt="Type: dev.synthwerk.pulse.target.status_changed.v1, one per stored change. Path: Transactional outbox to NATS JetStream stream EVT_PULSE, with the Go kit from synthwerk-contracts go/v0.1.0" src="assets/readme/events-v2-dark.svg" width="100%">
</picture>
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/config-v2-light.svg">
  <img alt="SYNTHWERK_PULSE_DATABASE_URL: Postgres URL. Required. A secret: never in the repo. SYNTHWERK_PULSE_NATS_URL: NATS JetStream URL. SYNTHWERK_PULSE_TARGETS_FILE: The targets file. Read at start. SYNTHWERK_PULSE_HTTP_ADDR: Listen address. Default :8080. SYNTHWERK_PULSE_DEFAULT_INTERVAL: Check interval. Default 15s. 1 s to 5 min. SYNTHWERK_PULSE_DEFAULT_TIMEOUT: Check timeout. Default 3s. Max the interval and 10 s. SYNTHWERK_PULSE_SSE_MAX_CLIENTS: Max open streams. Default 50" src="assets/readme/config-v2-dark.svg" width="100%">
</picture>
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/config2-v2-light.svg">
  <img alt="SYNTHWERK_PLATFORM_ENV: Environment name, such as local. SYNTHWERK_PULSE_LOG_LEVEL: Log level. SYNTHWERK_PULSE_LOG_FORMAT: Log format. OTEL_EXPORTER_OTLP_ENDPOINT: OTLP endpoint for traces, gRPC" src="assets/readme/config2-v2-dark.svg" width="100%">
</picture>
</p>

<p align="center">
<a href="https://github.com/VelimirMueller/synthwerk-contracts"><picture><source media="(prefers-color-scheme: light)" srcset="assets/readme/links-4-v2-light.svg"><img alt="Contracts (https://github.com/VelimirMueller/synthwerk-contracts)" src="assets/readme/links-4-v2-dark.svg" width="32%"></picture></a>
<a href="https://github.com/VelimirMueller/synthwerk-infra"><picture><source media="(prefers-color-scheme: light)" srcset="assets/readme/links-5-v2-light.svg"><img alt="Infra (https://github.com/VelimirMueller/synthwerk-infra)" src="assets/readme/links-5-v2-dark.svg" width="32%"></picture></a>
<a href="https://github.com/VelimirMueller/synthwerk"><picture><source media="(prefers-color-scheme: light)" srcset="assets/readme/links-6-v2-light.svg"><img alt="Overview (https://github.com/VelimirMueller/synthwerk)" src="assets/readme/links-6-v2-dark.svg" width="32%"></picture></a>
</p>

<br>

<p>
<img alt="05 STATUS. DRAFT. THE M1 SKELETON." src="assets/readme/divider-status-v2.svg" width="100%">
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/status-v2-light.svg">
  <img alt="http and tcp checks: beta. SSE stream and snapshot: beta. Status events via outbox: beta. Token check on the stream: with identity. Leader lock, sys.health fan-out: not yet. Incidents, metrics, KPIs, audit: not yet" src="assets/readme/status-v2-dark.svg" width="100%">
</picture>
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/now-v2-light.svg">
  <img alt="No token yet: The local edge binds to 127.0.0.1. The operator role check comes with identity. One process: No leader lock and no fan-out over sys.health.&gt; yet. Shared database: Pulse uses pulse_ tables in the shared database. Its own database comes with the infra module. Not yet: No incidents, metrics, alerts, KPIs or audit consumer" src="assets/readme/now-v2-dark.svg" width="100%">
</picture>
</p>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/checks-v2-light.svg">
  <img alt="tests/unit: status, check, target, stream, monitor. tests/integration: testcontainers Postgres + NATS: outbox events checked against the contract schemas, live down and up over SSE. ci: Blueprint go-ci: gofmt, golangci-lint, go test -race, build. image: Build, SBOM, Trivy. On main and tags: push to ghcr.io/velimirmueller/synthwerk-pulse, sign with cosign. gate: One required check over all jobs" src="assets/readme/checks-v2-dark.svg" width="100%">
</picture>
</p>

<details>
<summary><picture><source media="(prefers-color-scheme: light)" srcset="assets/readme/links-7-v2-light.svg"><img alt="Plain text" src="assets/readme/links-7-v2-dark.svg" width="32%"></picture></summary>

```bash
just lint         # gofmt, golangci-lint, actionlint, zizmor
just test-unit    # go test -race ./tests/unit/...
just test-int     # go test -tags integration -race ./tests/integration/...
```

</details>

<p>
<picture>
  <source media="(prefers-color-scheme: light)" srcset="assets/readme/footer-v2-light.svg">
  <img alt="VM. studio / flagship · open source · look per vm-brand playbook · MIT (c) 2026 Velimir Mueller · -- EOF -- two bad checks, then red --" src="assets/readme/footer-v2-dark.svg" width="100%">
</picture>
</p>
