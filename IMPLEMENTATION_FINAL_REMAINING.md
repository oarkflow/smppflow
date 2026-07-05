# Final Remaining Implementation

This pass adds the remaining operational modules that were still missing after the SMPP, manager, compliance, reporting and reconciliation layers.

## Added packages

- `pkg/ratelimit`
  - hierarchical policy model for global, tenant, provider and route limits
  - token bucket limiter
  - queue/backpressure decision support
  - adaptive throttling hooks for provider `ESME_RTHROTTLED` feedback

- `pkg/optout`
  - STOP/START/HELP keyword processor
  - tenant-scoped opt-out store
  - normalized MSISDN handling
  - inbound MO automation helper

- `pkg/contacts`
  - address book/contact store
  - CSV import
  - tag/country/operator segmentation
  - campaign-recipient targeting primitive

- `pkg/alerts`
  - operational alert rules
  - signal evaluation
  - incident open/close lifecycle
  - severity support for queue, DLR, provider, billing and SLA alerts

- `pkg/invoice`
  - invoice builder from billing-rated messages
  - tax calculation
  - provider/country line grouping
  - CSV invoice export

## API additions

The HTTP API now includes manager endpoints for:

- tenants
- binds
- providers
- sender approvals
- sessions
- audit logs
- DLQ replay
- queued job cancellation
- opt-outs
- contacts
- rate-limit policies
- open alerts

## Storage migration additions

The SQL migration now includes tables for:

- contacts
- rate policies
- alert rules
- incidents
- invoices

## Example

`examples/final_remaining` demonstrates rate limiting, STOP keyword handling, contact segmentation, alert incidents and invoice generation.

## Validation

Run:

```bash
go test ./...
```
