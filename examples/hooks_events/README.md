# hooks_events

This runnable example is part of the `smppflow` SMPP platform examples. It can be executed directly with `go run` and demonstrates one focused feature area while reusing the same protocol, routing, message, queue, storage, events, and hook components used by the larger examples.

## What it demonstrates

- How this feature composes with the core SMPP client/server protocol stack.
- How to use the package APIs from a standalone Go program.
- How to run the example locally without external SMPP vendor access.
- Expected console output showing route choices, submitted messages, protocol operations, reports, credentials, or lifecycle events depending on the example.

## Run

From the repository root:

```bash
go run ./examples/hooks_events
```

Or from this directory:

```bash
go run .
```

## Notes

- Examples that need an SMSC start the in-process mock SMPP server on a local ephemeral port.
- Provider examples use synthetic credentials and local addresses for safe offline execution.
- The mock server supports bind, submit, DLR, query, replace, cancel, data_sm, submit_multi, and SMPP 5 broadcast-style operations where used.
- Output values such as generated message IDs, timestamps, API key prefixes, generated DLRs, and quality scores are expected to change between runs.

## Production adaptation checklist

- Replace mock provider addresses with actual SMSC host:port values.
- Store provider credentials in a secure adapter instead of hardcoding them.
- Set per-provider TPS, window size, timeout, multipart mode, DLR format, and SMPP version profile.
- Add tenant/user-specific routing rules and quality statistics.
- Enable durable storage for messages, segments, PDUs, DLRs, queues, users, provider credentials, and audit logs.
- Add structured logs, metrics, tracing, and alerting around this flow.
