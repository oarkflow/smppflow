# Phone intelligence example

Demonstrates country-aware validation and enrichment using `github.com/oarkflow/phone@v0.0.7` through `intelligence.Resolver`.

```bash
go run ./examples/phone_intelligence
```

Expected output includes normalized E.164, national/international formats, country, MCC, MNC, NNC/NDC, carrier/network and blacklist/enricher metadata.
