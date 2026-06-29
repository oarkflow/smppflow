# Address detection example

Demonstrates automatic SMPP address format detection and TON/NPI inference for source and destination addresses.

It detects common production formats:

- alphanumeric sender IDs such as `ACME` or `ORGWARE`
- E.164 addresses with `+`
- international addresses with `00` prefix
- national addresses with trunk prefix `0`
- subscriber numbers
- short codes
- internet/email-style addresses

Run:

```bash
cd smppflow
go run ./examples/address_detection
```

Expected output is similar to:

```text
input="ACME"                 role=source      format=alphanumeric normalized="ACME"          ton=alphanumeric   npi=unknown
input="+977 980-000-0000"   role=destination format=e164          normalized="9779800000000" ton=international  npi=isdn_e164
...
submit_sm source={TON:alphanumeric NPI:unknown Addr:ORGWARE} destination={TON:international NPI:isdn_e164 Addr:9779800000000}
```

`BuildSubmitSM`, `BuildDataSM`, `BuildSubmitMulti`, `BuildQuerySM`, `BuildReplaceSM`, `BuildCancelSM`, broadcast operations, and alert notifications all auto-resolve addresses before PDU encoding. Explicit non-zero TON/NPI values are preserved, while visual separators and prefixes are normalized.
