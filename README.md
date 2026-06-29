# smppflow

`smppflow` is a production-oriented SMPP server/client platform in Go. It implements SMPP 3.3, 3.4 and 5.0 protocol coverage, server/client sessions, routing, message management, multipart SMS handling, DLR parsing, queueing, storage interfaces, hooks/events, reporting, and operational components for carrier-grade SMS gateways.

## Implemented capability map

### Protocol

- SMPP versions: 3.3, 3.4, 5.0.
- Full command constants and builders/parsers for common operations:
  - bind receiver/transmitter/transceiver
  - unbind
  - enquire_link
  - generic_nack
  - submit_sm
  - submit_multi
  - deliver_sm
  - data_sm
  - query_sm
  - cancel_sm
  - replace_sm
  - outbind
  - alert_notification
  - broadcast_sm
  - query_broadcast_sm
  - cancel_broadcast_sm
- Version capability matrix through `protocol.Capabilities`.
- Version validation through `protocol.ValidatePDUForVersion`.
- Version negotiation through `protocol.NegotiateVersion`.
- Complete standard TLV registry with v3.4/v5.0 metadata.
- v5-focused TLVs for congestion state, billing, endpoint identification, USSD and number portability.
- PDU codec with max-length protection.
- TON/NPI address auto-detection.

### Client/session runtime

- SMPP TCP/TLS client.
- Bind, reconnect primitives, heartbeat, enquire_link handling.
- Windowed request/response tracking.
- Submit, data_sm, submit_multi, query, cancel, replace, broadcast APIs.
- Async inbound PDU channel.
- Managed provider session manager with health, bind latency and failure tracking.

### Server runtime

- Runnable SMPP server/SMSC simulator.
- Auth hook.
- Bind handling and version negotiation.
- submit_sm/data_sm/query/cancel/replace/submit_multi/broadcast handlers.
- Auto DLR support.
- Alert/outbind PDU coverage.
- Production account wrapper package with hashed credentials, bind modes, bind limits and session registry primitives.

### Message platform

- Message manager.
- GSM 7-bit subset and UCS-2 detection.
- Multipart UDH and SAR TLV segmentation.
- Emoji/Unicode-safe UCS-2 encoding.
- Inbound deliver_sm handling.
- DLR parsing from text and TLVs.
- Segment records with provider message IDs.
- Lifecycle states for created, segmented, queued, submitted, accepted, delivered, failed, expired, rejected, dead-lettered and archived.

### Routing

- Provider registry.
- Tenant, user, sender, prefix and fallback routing.
- Quality/cost/latency scoring.
- Advanced strategies:
  - best quality
  - least cost
  - fastest latency
  - weighted routing
- Provider cooldown support.
- Provider stats for TPS, delivery rate, failures, latency and cost.

### Storage and queue

- Memory message/DLR store.
- Portable `database/sql` store abstraction for PostgreSQL/MySQL/SQLite style drivers supplied by the application.
- SQL migrations for messages and DLRs.
- Memory durable-like queue with retry/dead-letter behavior.
- Queue workers in service layer.

### Security

- Secret hashing and constant-time comparison.
- IP allow/deny CIDR policy.
- Token-bucket rate limiter.
- Redaction helper.
- Account model with tenant ID, system ID, allowed bind modes, source policy and TPS metadata.

### Observability/reporting

- Event bus with wildcard subscription.
- Lifecycle hooks.
- Prometheus-style metrics registry.
- Summary reports.
- API server exposing send, enqueue, list messages, list DLRs, reports and health.

### Number intelligence

- Country-aware phone parsing and formatting using `github.com/oarkflow/phone@v0.0.7`.
- `phone.Parse(number, region)`, `phone.Format`, and `phone.Verify` backed validation path.
- E.164, national, international and RFC3966 formatted output.
- Prefix/enrichment overlays for country, MCC, MNC, NNC/NDC, network, carrier, type and line type.
- Blacklist/whitelist primitives.
- HLR, MNP, carrier, roaming and fraud lookup extension hooks via `intelligence.Enricher`.

## Quick start

```bash
go test ./...
go run ./examples/mockserver
go run ./examples/client
go run ./examples/production_platform
go run ./examples/phone_intelligence
```


## Phone intelligence example

```go
import phone "github.com/oarkflow/phone"

num, err := phone.Parse("6502530000", "US")
formattedNum := phone.Format(num, phone.NATIONAL)
verified := phone.Verify("+1 650 253 0000")

_ = err
_ = formattedNum
_ = verified
```

The SMPP platform wraps the same library through `intelligence.Resolver`:

```go
resolver := intelligence.NewResolverWithRegion([]intelligence.PrefixEntry{
    {Prefix: "+1650", CountryCode: "US", MCC: "310", MNC: "260", NNC: "650", Network: "US mobile", Carrier: "T-Mobile"},
}, "US")

info := resolver.LookupWithRegion("6502530000", "US")
fmt.Println(info.E164, info.CountryCode, info.MCC, info.MNC, info.Network, info.Carrier)
```

## Example: protocol capability validation

```go
cap := protocol.Capabilities(protocol.Version50)
fmt.Println(cap.SupportsCellBroadcast) // true

err := protocol.ValidatePDUForVersion(protocol.Version34, protocol.PDU{
    CommandID: protocol.BroadcastSM,
})
// err: broadcast_sm is not supported by SMPP 3.4
```

## Example: provider routing

```go
r := routing.New()
r.RegisterProvider(provider.Modern50("p-fast", "127.0.0.1:2775", "sys", "pw"))
r.RegisterProvider(provider.Generic34("p-cheap", "127.0.0.1:2776", "sys", "pw"))
r.UpdateProviderStats(routing.ProviderStats{ProviderID: "p-fast", QualityScore: 98, AvgLatency: 20*time.Millisecond, CostPerSegment: .02})
r.UpdateProviderStats(routing.ProviderStats{ProviderID: "p-cheap", QualityScore: 90, AvgLatency: 80*time.Millisecond, CostPerSegment: .005})

ar := routing.NewAdvanced(r, routing.Policy{Strategy: routing.StrategyLeastCost})
decision, _ := ar.Route(ctx, message.SubmitRequest{From: "Brand", To: "+9779841000000", Text: "hello"})
fmt.Println(decision.Provider.ID)
```

## Example: SQL store

```go
// Import the DB driver in your application, then pass *sql.DB.
st := storage.NewSQL(db)
_ = st.Migrate(ctx)
```

The SQL implementation uses `database/sql` and does not force a specific driver. The default migration is intentionally simple and suitable for SQLite/PostgreSQL-style development. For MySQL or strict PostgreSQL production deployments, keep the interface and adjust the migration DDL in your app migration tool.

## Examples

- `examples/all_operations` — all SMPP command builders/parsers.
- `examples/advanced_protocol` — protocol matrix and advanced command coverage.
- `examples/api_server` — REST API over the service layer.
- `examples/async_service` — queue worker based sending.
- `examples/client` — SMPP client.
- `examples/mockserver` — runnable SMPP server.
- `examples/provider_routing` — routing rules.
- `examples/routing_queue` — route plus queue flow.
- `examples/text_manager` — encoding and multipart SMS.
- `examples/production_platform` — account, routing, metrics and number intelligence components.

## Production notes

This codebase now contains concrete implementations for the core SMPP platform layers and passes `go test ./...`. A live carrier deployment still needs environment-specific work that cannot be truthfully pre-certified in a static zip: vendor certification fixtures, real carrier DLR samples, real HLR/MNP/carrier intelligence adapters, real DB driver selection, TLS certificates, operational runbooks, load testing against the target throughput, and country-specific regulatory policy data.


## Essential production features still requiring live environment integration

The codebase now has protocol, routing, session, queue, storage, phone intelligence, metrics and server/client foundations. These remaining items are essential for a real carrier-grade deployment because they require live provider contracts, infrastructure credentials or regulatory inputs that cannot be truthfully completed in a generic archive:

1. **Live SMPP vendor certification packs**: provider-specific bind settings, TLV quirks, TON/NPI rules, DLR samples, throttling behavior and acceptance tests for each carrier/aggregator.
2. **HLR/MNP/roaming adapters**: commercial lookup integrations to confirm active SIM, current carrier, ported carrier, roaming state, reachability and fraud signals.
3. **Regulatory country rule database**: sender ID registration, template approval, DLT/10DLC/TCPA/GDPR/local opt-out rules, quiet hours and per-country content restrictions.
4. **Production secret/KMS integration**: HashiCorp Vault, cloud KMS/HSM, credential rotation workflow and encrypted-at-rest provider secrets.
5. **Real durable queue backends**: Redis Streams, NATS JetStream, Kafka, RabbitMQ or PostgreSQL advisory-lock workers with deployment-specific tuning.
6. **HA clustering**: distributed bind ownership, active/active gateway coordination, leader election, partition-aware routing and zero-downtime rolling rebinds.
7. **Billing/rating engine**: contract pricing, segment cost, FX, margins, invoice exports, balance reservation and prepaid/postpaid credit control.
8. **Abuse/compliance operations**: spam scoring, URL/domain reputation, sender spoof prevention, DND lists, STOP/HELP automation and content moderation policy.
9. **Provider SLA automation**: live canary traffic, route cooling, blackhole detection, latency SLOs and automatic provider incident creation.
10. **Full external observability stack**: OpenTelemetry collector, Prometheus/Grafana dashboards, log pipeline, alert rules and long-retention reporting warehouse.
11. **Packet-level interoperability lab**: tcpdump/Wireshark fixtures, malformed PDU replay, fuzz corpus, high-TPS soak testing and vendor acceptance scripts.
12. **Admin UI and operator workflows**: bind/session viewer, route simulator, DLQ replay, DLR trace, account management, sender approval and provider maintenance controls.

## Implemented remaining production modules

This archive now includes concrete packages for the remaining carrier-grade platform capabilities:

| Area | Package / files | What is implemented |
|---|---|---|
| Live SMPP certification packs | `certification` | Provider pack model, bind/window/TPS settings, TLV quirks, TON/NPI rules, DLR samples, throttling behavior, JSON import/export, submit validation and acceptance-case model. Real vendor-certified values must come from the carrier contract/interconnect document; the framework executes them without code changes. |
| HLR/MNP/roaming adapters | `lookup` | Chainable lookup adapters, `phone`-backed normalization, static HLR/MNP fixture adapter, HTTP JSON adapter for commercial HLR/MNP/SIM-swap/reachability/fraud APIs, result merge semantics. |
| Regulatory country rules | `regulation` | Country rules for sender registration, template approval, DLT/10DLC/TCPA/GDPR/local opt-out flags, DND/opt-out lists, quiet hours, blocked content, required footers, registered template regexes. |
| Production secrets/KMS | `secrets` | AES-GCM local KMS, encrypted secret store, rotation workflow, HashiCorp Vault transit adapter, redaction helpers. Cloud KMS/HSM integrations can implement the same `KMS` interface. |
| Durable queues | `durablequeue` | `Backend` interface, PostgreSQL/SKIP LOCKED worker queue, Redis Streams RESP adapter, external backend wrapper for NATS JetStream, Kafka and RabbitMQ client bindings. |
| HA clustering | `cluster` | Node heartbeat, bind lease ownership, TTL renewal/release, active/draining/down states, partition-aware key owner selection and zero-downtime drain primitive. |
| Billing/rating | `billing` | Contract price rules, segment cost/sell/margin, FX conversion table, prepaid balance reservation/capture/release, invoice CSV export. |
| Abuse/compliance operations | `compliance` | Spam/content regex scoring, URL extraction, domain reputation scoring, sender verification/anti-spoof policy, STOP/HELP automation, opt-out handling, content fingerprinting. |
| Provider SLA automation | `sla` | Canary sender interface, SLO definitions, blackhole detection, provider cooldown, router quality penalty, incident sink integration. |
| External observability stack | `observability`, `observability/deploy` | OpenTelemetry/Prometheus/Grafana/alert config models and deployment files: `otel-collector.yaml`, `prometheus.yml`, `alerts.yml`, `grafana-dashboard.json`. |
| Packet-level interoperability lab | `lab` | Hex fixture load/save, malformed PDU replay, fuzz corpus mutation, tcpdump wrapper, high-TPS soak test harness. |

Run the combined example:

```bash
go run ./examples/remaining_features
```

Run the full validation suite:

```bash
go test ./...
```

### Important deployment note

The code includes complete adapter contracts and runnable local implementations. For live SMPP vendor certification, HLR/MNP/SIM-swap providers, Vault, Redis, PostgreSQL, NATS, Kafka, RabbitMQ and observability backends, production credentials/endpoints and vendor-specific certification documents are required at deployment time. Those commercial values cannot be truthfully embedded in an open archive, but the packages are ready to consume them through JSON/config/interfaces without changing core code.
