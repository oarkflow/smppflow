package main

import (
	"fmt"

	"github.com/oarkflow/smppflow/protocol"
)

func main() {
	samples := []struct {
		addr string
	}{
		{"ACME"},
		{"+977 980-000-0000"},
		{"009779800000000"},
		{"01444123"},
		{"12345"},
		{"user@example.com"},
	}

	for _, sample := range samples {
		a := protocol.DetectAddress(sample.addr)
		fmt.Printf("input=%-22q format=%-14s normalized=%-15q ton=%-14s npi=%s\n", sample.addr, a.Format, a.Normalized, a.TON, a.NPI)
	}

	pdu := protocol.BuildSubmitSM(1, protocol.SubmitSMRequest{
		Source:       protocol.Address{Addr: "ORGWARE"},
		Destination:  protocol.Address{Addr: "+977 980-000-0000"},
		DataCoding:   protocol.DataCodingDefault,
		ShortMessage: []byte("auto detected TON/NPI"),
	})
	parsed, err := protocol.ParseSubmitSM(pdu)
	if err != nil {
		panic(err)
	}
	fmt.Printf("submit_sm source=%+v destination=%+v\n", parsed.Source, parsed.Destination)
}
