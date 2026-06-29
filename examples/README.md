# smppflow examples

Every example is runnable with `go run ./examples/<name>` from the repository root. The examples are intentionally small but complete: examples that require SMPP connectivity start the included mock SMSC locally, and examples that demonstrate provider registration or credential storage use in-memory adapters with redacted output.

## Example index

| Example | Purpose | Run |
|---|---|---|
| `advanced_protocol` | Exercises advanced protocol builders and client calls such as `data_sm` and `query_sm`. | `go run ./examples/advanced_protocol` |
| `address_detection` | Demonstrates address format detection and automatic TON/NPI inference for source, destination, short code, national, international, E.164, and internet addresses. | `go run ./examples/address_detection` |
| `all_features` | End-to-end service flow: mock SMSC, events, hooks, routing, service, queue, DLR, storage, and reports. | `go run ./examples/all_features` |
| `all_operations` | Exercises the broad SMPP operation set including submit, submit_multi, data_sm, query, replace, cancel, broadcast operations, outbind, and alert notification. | `go run ./examples/all_operations` |
| `api_server` | Starts the HTTP API service for send, enqueue, message listing, DLR listing, summary reporting, and health checks. | `go run ./examples/api_server` |
| `async_service` | Demonstrates queued asynchronous SMS submission and worker processing. | `go run ./examples/async_service` |
| `client` | Demonstrates a raw SMPP client talking to the local mock SMSC. | `go run ./examples/client` |
| `credentials_store` | Demonstrates user storage, API-key authentication, provider credential storage, and redaction. | `go run ./examples/credentials_store` |
| `hooks_events` | Demonstrates hook registration and event-bus subscription. | `go run ./examples/hooks_events` |
| `mockserver` | Starts a local mock SMSC/server for manual client testing. | `go run ./examples/mockserver` |
| `protocol_matrix` | Prints command/version coverage for SMPP 3.3, 3.4, and 5.0. | `go run ./examples/protocol_matrix` |
| `provider_registration` | Demonstrates multi-provider registration, tenant/user storage, API-key auth, credentials, provider stats, and rules. | `go run ./examples/provider_registration` |
| `provider_routing` | Demonstrates fast indexed provider routing by tenant, user, sender, recipient prefix, quality, latency, TPS, and cost. | `go run ./examples/provider_routing` |
| `reports` | Demonstrates JSON and CSV report generation from stored messages and DLRs. | `go run ./examples/reports` |
| `routing_queue` | Demonstrates basic routing and the priority/delayed queue. | `go run ./examples/routing_queue` |
| `text_manager` | Demonstrates GSM/UCS2 analysis, invisible character detection, and automatic segmentation. | `go run ./examples/text_manager` |

## Running all non-blocking examples

Some examples intentionally run long-lived servers (`api_server`, `mockserver`). To run the non-blocking examples:

```bash
for ex in address_detection advanced_protocol all_features all_operations async_service client credentials_store hooks_events protocol_matrix provider_registration provider_routing reports routing_queue text_manager; do
  echo "== $ex =="
  go run ./examples/$ex
done
```

## Production mapping

The examples use memory adapters for clarity. In production, replace them with durable implementations behind the same interfaces:

- account/user credential store: PostgreSQL, MySQL, SQLite, Redis, Vault-backed secret storage, or your own adapter.
- message/DLR store: PostgreSQL or another durable store.
- queue: durable queue with visibility timeout, retries, and DLQ.
- provider profiles: configuration database or hot-reloaded config.
- provider stats: metrics pipeline that updates quality, delivery rate, latency, TPS, and cost.
