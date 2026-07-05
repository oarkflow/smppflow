# smppflow

`smppflow` is a Go SMPP platform with a focused runnable demo containing only two examples:

```text
examples/server  # SMPP SMSC simulator + HTTP API + routing + queue + reporting
examples/client  # HTTP API client + low-level SMPP client
```

All older examples were removed so the project has a single clear way to run and verify the platform.

## What the demo covers

The server example starts both:

- an SMPP listener on `:2775`
- an HTTP API listener on `:8080`

It wires:

- SMPP bind authentication
- `submit_sm`, `deliver_sm`, `data_sm`, `query_sm`, `submit_multi`, `replace_sm`, `cancel_sm`
- automatic DLR simulation
- async queue workers
- routing with a single production router
- route validation
- capacity reservation
- circuit breaker and SLA feedback
- message lifecycle and DLR persistence
- manager/admin endpoints
- reports and NDJSON export
- contacts, opt-outs, rate-limit policies, alerts
- structured logging with full JSON bodies, no truncation/skipping

The client example verifies:

- HTTP health
- route simulation
- synchronous send
- async enqueue
- message/DLR/report/admin APIs
- low-level SMPP bind
- `submit_sm`
- inbound DLR `deliver_sm`
- `deliver_sm_resp`
- `query_sm`
- `data_sm`
- `submit_multi`
- `replace_sm`
- `cancel_sm`

## Logging format

Default logs are JSON lines using Go `log/slog` JSON handler.

Unlike the previous version, HTTP response bodies are logged as structured JSON objects/arrays instead of escaped JSON strings. The client also reads full response bodies; it does not truncate at 4 KB.

Example body logging now looks like this:

```json
{"msg":"http.get","url":"http://127.0.0.1:8080/v1/messages","status":200,"body":[{"ID":"msg-...","TenantID":"tenant-a","State":"delivered"}]}
```

For human text logs:

```bash
go run ./examples/server --log-format text
go run ./examples/client --log-format text
```

For machine-readable logs:

```bash
go run ./examples/server --log-format json
go run ./examples/client --log-format json
```

## Requirements

```bash
go version
```

Recommended:

```text
Go 1.23+
```

## Run tests

```bash
go test ./...
```

Expected:

```text
PASS
```

## Start the server

```bash
go run ./examples/server
```

Expected logs:

```json
{"msg":"smpp.server.start","addr":":2775","system_id":"smppclient"}
{"msg":"http.server.start","addr":":8080"}
{"msg":"ready","smpp":":2775","http":":8080"}
{"msg":"usage.health","command":"curl -s http://127.0.0.1:8080/healthz"}
{"msg":"usage.send","command":"curl -s -X POST http://127.0.0.1:8080/v1/messages/send ..."}
{"msg":"usage.client","command":"go run ./examples/client --http http://127.0.0.1:8080 --smpp 127.0.0.1:2775 ..."}
```

The server accepts this default SMPP bind:

```text
system_id: smppclient
password:  password
bind mode: transceiver
```

Custom ports:

```bash
go run ./examples/server \
  --smpp :29975 \
  --http :18080 \
  --provider-addr 127.0.0.1:29975
```

## Run the client

With default ports:

```bash
go run ./examples/client
```

HTTP-only:

```bash
go run ./examples/client --mode http
```

SMPP-only:

```bash
go run ./examples/client --mode smpp
```

Custom ports:

```bash
go run ./examples/client \
  --http http://127.0.0.1:18080 \
  --smpp 127.0.0.1:29975 \
  --system-id smppclient \
  --password password
```

## HTTP curl usage

### Health

```bash
curl -s http://127.0.0.1:8080/healthz
```

Expected:

```text
ok
```

### Route simulation

```bash
curl -s -X POST http://127.0.0.1:8080/v1/manager/route/simulate \
  -H 'Content-Type: application/json' \
  -d '{
    "TenantID":"tenant-a",
    "From":"BRAND",
    "To":"9779841000000",
    "Text":"Your OTP is 123456",
    "RegisteredDelivery":1,
    "Meta":{"country":"NP","operator":"NTC","message_type":"otp"}
  }' | jq
```

Expected response shape:

```json
{
  "request": {...},
  "decision": {
    "Provider": {"ID":"local-smpp"},
    "Rule": {"ID":"tenant-a-np-otp"},
    "Reason":"eligible",
    "ScoreBreakdown": {...}
  },
  "compliance_allowed": true,
  "rejected_providers": []
}
```

### Send synchronously

```bash
curl -s -X POST http://127.0.0.1:8080/v1/messages/send \
  -H 'Content-Type: application/json' \
  -d '{
    "TenantID":"tenant-a",
    "From":"BRAND",
    "To":"9779841000000",
    "Text":"Your OTP is 123456",
    "RegisteredDelivery":1,
    "UseSAR":true,
    "IdempotencyKey":"demo-key-1",
    "CampaignID":"demo-campaign",
    "Meta":{"country":"NP","operator":"NTC","message_type":"otp","campaign_id":"demo-campaign"}
  }' | jq
```

Expected response:

```json
{
  "MessageID": "msg-...",
  "ProviderIDs": ["smsc-000001"],
  "Parts": 1,
  "State": "submitted",
  "Idempotent": false
}
```

The server then emits a DLR automatically and the message becomes `delivered`.

### Enqueue asynchronously

```bash
curl -s -X POST http://127.0.0.1:8080/v1/messages/enqueue \
  -H 'Content-Type: application/json' \
  -d '{
    "TenantID":"tenant-a",
    "From":"BRAND",
    "To":"9779841000000",
    "Text":"Your OTP is 123456 queued",
    "RegisteredDelivery":1,
    "UseSAR":true,
    "IdempotencyKey":"queued-key-1",
    "CampaignID":"demo-campaign",
    "Priority":5,
    "Meta":{"country":"NP","operator":"NTC","message_type":"otp","campaign_id":"demo-campaign"}
  }' | jq
```

Expected:

```json
{"queued":true}
```

### List messages

```bash
curl -s http://127.0.0.1:8080/v1/messages | jq
```

Expected shape:

```json
[
  {
    "ID":"msg-...",
    "TenantID":"tenant-a",
    "ProviderID":"local-smpp",
    "ProviderMessageID":"smsc-000001",
    "State":"delivered",
    "Parts":[{"State":"delivered"}]
  }
]
```

### List DLRs

```bash
curl -s http://127.0.0.1:8080/v1/dlrs | jq
```

Expected shape:

```json
[
  {
    "ProviderID":"local-smpp",
    "ProviderMessageID":"smsc-000001",
    "State":"delivered",
    "ErrorCode":"000"
  }
]
```

### Reports

```bash
curl -s http://127.0.0.1:8080/v1/reports/summary | jq
curl -s 'http://127.0.0.1:8080/v1/reports/delivery?tenant_id=tenant-a&group_by=day' | jq
curl -s 'http://127.0.0.1:8080/v1/reports/providers?provider_id=local-smpp' | jq
curl -s 'http://127.0.0.1:8080/v1/reports/tenants?tenant_id=tenant-a' | jq
curl -s 'http://127.0.0.1:8080/v1/reports/errors?tenant_id=tenant-a' | jq
```

Expected summary shape:

```json
{
  "total": 2,
  "by_state": {"delivered": 2},
  "delivery_rate": 1,
  "pending_dlr": 0
}
```

### NDJSON export

```bash
curl -s http://127.0.0.1:8080/v1/reports/export.ndjson
```

Expected shape:

```json
{"kind":"message","tenant_id":"tenant-a","provider_id":"local-smpp","message_id":"msg-...","state":"delivered"}
{"kind":"dlr","provider_id":"local-smpp","state":"delivered"}
```

The client logs this endpoint as an array of decoded NDJSON objects so the log body is not escaped and is not truncated.

### Manager/admin endpoints

```bash
curl -s http://127.0.0.1:8080/v1/manager/tenants | jq
curl -s http://127.0.0.1:8080/v1/manager/providers | jq
curl -s http://127.0.0.1:8080/v1/manager/binds | jq
curl -s http://127.0.0.1:8080/v1/manager/senders | jq
curl -s http://127.0.0.1:8080/v1/manager/sessions | jq
curl -s http://127.0.0.1:8080/v1/manager/audit | jq
curl -s http://127.0.0.1:8080/v1/manager/queue | jq
curl -s http://127.0.0.1:8080/v1/manager/dlq | jq
curl -s http://127.0.0.1:8080/v1/contacts | jq
curl -s http://127.0.0.1:8080/v1/optouts | jq
curl -s http://127.0.0.1:8080/v1/ratelimits | jq
curl -s http://127.0.0.1:8080/v1/alerts/open | jq
```

## Low-level SMPP client behavior

Run:

```bash
go run ./examples/client --mode smpp
```

Expected flow:

```text
connect
bind_transceiver
submit_sm
receive deliver_sm DLR
send deliver_sm_resp
query_sm
data_sm
submit_multi
replace_sm
cancel_sm
unbind/close
```

Expected logs include:

```json
{"msg":"smpp.bound","state":"bound","window":8}
{"msg":"smpp.submit_sm.ok","provider_message_id":"smsc-000001"}
{"msg":"smpp.inbound_pdu","command":"deliver_sm","seq":...}
{"msg":"smpp.deliver_sm","from":"BRAND","to":"9779841000000","text":"id:smsc-000001 ... stat:DELIVRD ..."}
{"msg":"smpp.query_sm.ok","message_id":"smsc-000001","state":2,"error":0}
{"msg":"smpp.data_sm.ok","provider_message_id":"data-000002"}
{"msg":"smpp.submit_multi.ok","provider_message_id":"smsc-000003"}
{"msg":"smpp.replace_sm.ok","message_id":"smsc-000001"}
{"msg":"smpp.cancel_sm.ok","message_id":"smsc-000001"}
```

## Logging examples

### Previous problem

The previous client logged JSON bodies as strings:

```text
body="[{\"ID\":\"local-smpp\"}]"
```

It also read only the first 4096 bytes.

### Current behavior

The client now:

- reads the complete response body
- decodes JSON objects and arrays
- decodes NDJSON into an array of objects
- logs body as structured `slog.Any`
- uses JSON logs by default

Example:

```json
{
  "msg":"http.get",
  "url":"http://127.0.0.1:8080/v1/manager/providers",
  "status":200,
  "body":[
    {
      "ID":"local-smpp",
      "Name":"local-smpp",
      "SupportsQuery":true,
      "SupportsCancel":true,
      "SupportsReplace":true,
      "SupportsDataSM":true,
      "SupportsSubmitMulti":true
    }
  ]
}
```

## SMPP protocol support note

The implementation includes broad SMPP 3.3, 3.4, and 5.0 command support and validates version capabilities where implemented. It is suitable for local SMSC simulation, ESME client flows, async DLRs, routing, queueing, and carrier-integration development.

Real-world SMPP interoperability still depends on each carrier's certification profile because providers often differ on:

- bind mode
- TON/NPI rules
- DLR text format
- TLV requirements
- SAR vs UDH multipart behavior
- throttling behavior
- allowed source address policy
- unsupported optional commands

Use the included server/client example as the functional reference, then add provider-specific certification packs for live carrier rollout.

## Developer commands

```bash
go fmt ./...
go test ./...
go run ./examples/server
go run ./examples/client
```

