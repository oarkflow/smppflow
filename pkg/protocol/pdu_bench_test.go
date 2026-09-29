package protocol_test

// Benchmarks for PDU build/parse and wire encode/decode. These cover the
// per-message serialization cost that runs on every single SMS submitted or
// delivered through the platform.

import (
	"testing"

	"github.com/oarkflow/smppflow/pkg/codec"
	"github.com/oarkflow/smppflow/pkg/protocol"
)

func benchSubmitSMRequest() protocol.SubmitSMRequest {
	return protocol.SubmitSMRequest{
		ServiceType:        "",
		Source:             protocol.Address{TON: protocol.TONInternational, NPI: protocol.NPIISDN, Addr: "14155550100"},
		Destination:        protocol.Address{TON: protocol.TONInternational, NPI: protocol.NPIISDN, Addr: "9779800000000"},
		ESMClass:           0,
		ProtocolID:         0,
		PriorityFlag:       0,
		RegisteredDelivery: 1,
		DataCoding:         0,
		ShortMessage:       []byte("Hello, this is a benchmark SMS payload used to measure encode/decode cost."),
	}
}

func BenchmarkBuildSubmitSM(b *testing.B) {
	req := benchSubmitSMRequest()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = protocol.BuildSubmitSM(uint32(i+1), req)
	}
}

func BenchmarkParseSubmitSM(b *testing.B) {
	p := protocol.BuildSubmitSM(1, benchSubmitSMRequest())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := protocol.ParseSubmitSM(p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildDeliverSM(b *testing.B) {
	req := benchSubmitSMRequest()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = protocol.BuildDeliverSM(uint32(i+1), req)
	}
}

func BenchmarkParseDeliverSM(b *testing.B) {
	p := protocol.BuildDeliverSM(1, benchSubmitSMRequest())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := protocol.ParseSubmitSM(p); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodeSubmitSM measures the wire encode (PDU -> bytes) cost, which
// happens once per outbound PDU (every submit_sm, deliver_sm, resp, etc).
func BenchmarkEncodeSubmitSM(b *testing.B) {
	p := protocol.BuildSubmitSM(1, benchSubmitSMRequest())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := codec.Encode(p); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeSubmitSM measures the wire decode (bytes -> PDU) cost, which
// happens once per inbound PDU read off the socket.
func BenchmarkDecodeSubmitSM(b *testing.B) {
	p := protocol.BuildSubmitSM(1, benchSubmitSMRequest())
	raw, err := codec.Encode(p)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := codec.Decode(raw); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodeDecodeSubmitSMRoundTrip measures the full build->encode->decode->parse
// cycle a message goes through end to end at the protocol layer.
func BenchmarkEncodeDecodeSubmitSMRoundTrip(b *testing.B) {
	req := benchSubmitSMRequest()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := protocol.BuildSubmitSM(uint32(i+1), req)
		raw, err := codec.Encode(p)
		if err != nil {
			b.Fatal(err)
		}
		got, err := codec.Decode(raw)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := protocol.ParseSubmitSM(got); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildSubmitSMResp(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = protocol.BuildSubmitSMResp(uint32(i+1), protocol.ESME_ROK, "1234567890")
	}
}

func BenchmarkParseSubmitSMResp(b *testing.B) {
	p := protocol.BuildSubmitSMResp(1, protocol.ESME_ROK, "1234567890")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := protocol.ParseSubmitSMResp(p); err != nil {
			b.Fatal(err)
		}
	}
}
