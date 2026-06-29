# smppflow

`smppflow` is a comprehensive Go SMPP toolkit and platform foundation with SMPP client, mock SMSC/server, protocol codec, message/text manager, automatic address TON/NPI detection, automatic multipart handling, event bus, hooks, lifecycle states, storage, queues, routing, provider profiles, service layer, HTTP API, reports, and runnable examples.

The implementation is interface-first and intentionally extensible: provider adapters, PostgreSQL/MySQL/Redis stores, Kafka/NATS event streams, webhooks, country policies, dashboards, and certified vendor fixtures can be added without rewriting the protocol/session/message core.

## Implemented feature families

### SMPP protocol

- SMPP version constants: 3.3, 3.4, 5.0.
- Binary PDU header encode/decode.
- Command IDs and status codes.
- TLV registry and helpers.
- Unknown TLV preservation while parsing submit/deliver PDUs.
- Implemented/buildable operations:
  - `bind_receiver`, `bind_transmitter`, `bind_transceiver`
  - bind responses
  - `submit_sm`, `submit_sm_resp`
  - `deliver_sm`, `deliver_sm_resp`
  - `data_sm`, `data_sm_resp`
  - `query_sm`, `query_sm_resp`
  - `replace_sm`, `replace_sm_resp`
  - `cancel_sm`, `cancel_sm_resp`
  - `submit_multi`, `submit_multi_resp`
  - `enquire_link`, `enquire_link_resp`
  - `unbind`, `unbind_resp`
  - `generic_nack`
  - constants for alert/broadcast families

### SMPP client/session runtime

- TCP client.
- Optional TLS config.
- Bind as receiver/transmitter/transceiver.
- Windowed request/response tracking.
- Sequence manager with wrap protection.
- Heartbeat with `enquire_link`.
- Inbound PDU channel.
- Graceful unbind/close.
- Response timeout.
- Window-full protection.
- Methods for `SubmitSM`, `DataSM`, `QuerySM`, `CancelSM`, `ReplaceSM`.

### Mock SMPP server/SMSC

- Bind authentication hook.
- `submit_sm` handling.
- `data_sm` handling.
- `query_sm`, `replace_sm`, `cancel_sm` handling.
- `submit_multi` response.
- Auto DLR generation through `deliver_sm`.
- `enquire_link` and `unbind` handling.
- Custom handler hooks for submit/query/cancel/replace/data_sm.

### Text and SMS manager

- GSM 03.38 basic detection.
- GSM extension table detection.
- UCS-2 detection.
- Binary encoding mode.
- Data coding selection.
- Invisible/zero-width character detection.
- SMS part estimation.
- Automatic segmentation.
- UDH multipart concatenation.
- SAR TLV multipart concatenation.
- Segment references and part indexes.
- Segment-level provider message IDs.

### Message lifecycle

- Message creation.
- Validation.
- Encoding/segmentation.
- Submission.
- Segment-level state tracking.
- Parent message state.
- DLR processing loop.
- Inbound MO event publishing.
- Metadata and tenant fields.

### Delivery receipts

- Text DLR parsing: `id:... stat:... err:...`.
- TLV-based `receipted_message_id`.
- TLV-based `message_state`.
- Normalized states.
- Raw DLR preservation.
- DLR storage.

### Events and hooks

- In-memory event bus.
- Sync or async event handlers.
- Wildcard subscriptions.
- Ordered hooks with priorities.
- Hook timeout.
- Fail-open/fail-closed behavior.
- Panic protection.
- Hook points around connect, bind, PDU send, validation, segmentation, submit, DLR, inbound, and storage.

### Storage

- Storage interface.
- In-memory implementation.
- Message persistence.
- DLR persistence.
- State update.
- Safe copies for list/get operations.

### Queue and async service

- Memory queue.
- Immediate jobs.
- Delayed/scheduled jobs.
- Priority reservation.
- Retry with delay.
- Max attempts.
- Dead-letter queue.
- Async service workers.
- Direct send and enqueue APIs.

### Routing and provider profiles

- Provider profiles for generic SMPP 3.4, legacy SMPP 3.3, modern SMPP 5.0.
- Per-provider version, bind mode, TPS, window, encoding/multipart preferences, DLR format, capabilities.
- Routing by tenant, prefix, sender, priority.
- Fallback routing.

### Reports

- JSON summary report.
- CSV message export.
- Counts by message state.
- Parts count.
- DLR count.

### HTTP API service

- `POST /v1/messages/send`
- `POST /v1/messages/enqueue`
- `GET /v1/messages`
- `GET /v1/dlrs`
- `GET /v1/reports/summary`
- `GET /healthz`


## Automatic address format detection

All SMPP builders and the high-level message manager can infer TON/NPI from sender and destination address format when TON/NPI are not explicitly set. The detector normalizes common address decorations before encoding PDUs.

Supported detections include:

- alphanumeric sender IDs: `ORGWARE` -> TON `alphanumeric`, NPI `unknown`
- E.164 addresses: `+977 980-000-0000` -> normalized `9779800000000`, TON `international`, NPI `isdn_e164`
- international `00` prefix: `009779800000000` -> normalized `9779800000000`, TON `international`, NPI `isdn_e164`
- national trunk prefix: `01444123` -> TON `national`, NPI `isdn_e164`
- short codes: `12345` -> TON `abbreviated`, NPI `unknown`
- subscriber numbers: `1234567` -> TON `subscriber`, NPI `isdn_e164`
- internet/email style addresses: `user@example.com` -> NPI `internet`

Explicit non-zero TON/NPI values are preserved, while the address string is still normalized. Run:

```bash
go run ./examples/address_detection
```

## Runnable examples

```bash
go run ./examples/mockserver
```

```bash
go run ./examples/client
```

```bash
go run ./examples/all_features
```

```bash
go run ./examples/async_service
```

```bash
go run ./examples/advanced_protocol
go run ./examples/address_detection
```

```bash
go run ./examples/text_manager
```

```bash
go run ./examples/hooks_events
```

```bash
go run ./examples/routing_queue
```

```bash
go run ./examples/reports
```

```bash
go run ./examples/api_server
```

API example request after starting `examples/api_server`:

```bash
curl -s http://127.0.0.1:8088/v1/messages/send \
  -H 'content-type: application/json' \
  -d '{"tenant_id":"tenant-a","from":"OARK","to":"9779800000000","text":"Hello via API 👋","registered_delivery":1}'
```

## Test

```bash
go test ./...
```

## Package map

```text
api/           HTTP API wrapper
codec/         binary PDU codec
events/        event bus
hooks/         hook manager
lifecycle/     connection/message lifecycle states
message/       message manager, text manager, DLR parser
protocol/      SMPP types, constants, PDU builders/parsers, TLVs
provider/      provider/vendor capability profiles
queue/         durable-style in-memory priority/retry/DLQ queue
reports/       JSON/CSV reports
routing/       provider routing engine
server/        mock SMSC/server
service/       high-level async SMS service
session/       SMPP client/session runtime
storage/       storage interfaces and memory store
examples/      runnable examples for features, clients, services
```

## Production extension checklist

The current package is runnable and tested. For a live carrier-certified deployment, add your vendor-specific modules through the included interfaces:

- PostgreSQL/MySQL/Redis storage implementation.
- Kafka/NATS/Redis event stream implementation.
- Provider-certified DLR parsers and message ID normalizers.
- Country/regulatory validators.
- Credential vault integration.
- Webhook dispatcher with signing and retry.
- XLSX/PDF report writers if needed.
- Full carrier certification fixtures for each provider.

## Complete SMPP operation coverage

This build includes an executable operation matrix for SMPP 3.3, 3.4 and 5.0 in `protocol.AllOperations` with runtime helpers:

- `protocol.SupportsCommand(version, command)`
- `protocol.CommandsForVersion(version)`
- `protocol.OperationByCommand(command)`

Covered command families:

- Session: `bind_receiver`, `bind_transmitter`, `bind_transceiver`, `outbind`, `unbind`, `enquire_link`, `generic_nack`
- Messaging: `submit_sm`, `submit_multi`, `deliver_sm`, `data_sm`
- Message management: `query_sm`, `replace_sm`, `cancel_sm`
- Notifications: `alert_notification`
- SMPP 5.0 broadcast: `broadcast_sm`, `query_broadcast_sm`, `cancel_broadcast_sm`

Runnable coverage examples:

```bash
go run ./examples/protocol_matrix
go run ./examples/all_operations
```

`examples/all_operations` starts a local mock SMPP 5.0 SMSC, binds a transceiver client, and exercises submit, multi-submit, data_sm, query, replace, cancel, broadcast, query broadcast, cancel broadcast, outbind builder/parser, and alert notification builder/parser.

## Version behavior

- SMPP 3.3 exposes transmitter/receiver binds and classic submit/query/replace/cancel/deliver operations.
- SMPP 3.4 adds transceiver bind, submit_multi, data_sm, outbind, alert_notification and TLV-heavy interoperability.
- SMPP 5.0 adds broadcast command coverage in addition to the SMPP 3.4 command family.

The mock server now validates commands against the bound interface version and returns `generic_nack` with `ESME_RINVCMDID` when a command is not valid for that session version.
storage/       message/DLR storage interfaces and memory adapter
account/       user, API-key, and provider credential storage adapter
```

## New provider routing and credential capabilities

The platform now includes a production-style routing layer intended for multiple providers and multiple tenants/users.

### Provider registration

Providers are registered through `routing.Router.RegisterProvider` or `service.Service.RegisterProvider`. A provider profile contains SMPP version, bind mode, TPS, window size, multipart mode, DLR behavior, capabilities, TON/NPI defaults, TLS preference, and vendor metadata.

### User and credential storage

The `account` package provides the storage adapter contract and a safe in-memory implementation for:

- users
- roles
- tenant IDs
- API keys with hashed secrets
- API key authentication
- provider credentials per tenant/provider
- provider credential redaction for logs, examples, and admin output

Production systems can replace `account.Memory` with a PostgreSQL, MySQL, SQLite, Redis, Vault, or KMS-backed adapter without changing the service and routing code.

### Fast provider routing

The routing engine now supports indexed candidate selection and scoring by:

- tenant
- user ID from `SubmitRequest.Meta["user_id"]`
- recipient prefix/country route
- sender ID
- provider priority
- provider weight
- provider quality score
- delivery rate
- average latency
- current TPS and max TPS
- cost per segment
- minimum quality policy
- maximum cost policy
- fallback quality routing

This gives predictable route selection while remaining fast enough for high-throughput SMS submission pipelines. The router keeps indexes for tenant, tenant+sender, tenant+user, and longest recipient prefixes to avoid scanning every rule for common cases.

### Examples for this feature

```bash
go run ./examples/provider_routing
```

```bash
go run ./examples/provider_registration
```

```bash
go run ./examples/credentials_store
```

See [`examples/README.md`](examples/README.md) and each example directory README for usage notes and production adaptation checklists.
