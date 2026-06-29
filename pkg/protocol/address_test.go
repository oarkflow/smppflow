package protocol

import "testing"

func TestDetectAddressFormatsAndTONNPI(t *testing.T) {
	cases := []struct {
		in     string
		format AddressFormat
		norm   string
		ton    TON
		npi    NPI
	}{
		{"BRAND", AddressFormatAlphanumeric, "BRAND", TONAlphanumeric, NPIUnknown},
		{"+977 980-000-0000", AddressFormatE164, "9779800000000", TONInternational, NPIISDN},
		{"009779800000000", AddressFormatInternational, "9779800000000", TONInternational, NPIISDN},
		{"9800000000", AddressFormatInternational, "9800000000", TONInternational, NPIISDN},
		{"01444123", AddressFormatNational, "01444123", TONNational, NPIISDN},
		{"12345", AddressFormatShortCode, "12345", TONAbbreviated, NPIUnknown},
		{"1234567", AddressFormatSubscriber, "1234567", TONSubscriber, NPIISDN},
		{"user@example.com", AddressFormatInternet, "user@example.com", TONUnknown, NPIInternet},
		{"tel:+1 (415) 555-2671", AddressFormatE164, "14155552671", TONInternational, NPIISDN},
	}
	for _, tc := range cases {
		a := DetectAddress(tc.in)
		if a.Format != tc.format || a.Normalized != tc.norm || a.TON != tc.ton || a.NPI != tc.npi {
			t.Fatalf("DetectAddress(%q)=%+v want format=%s norm=%s ton=%s npi=%s", tc.in, a, tc.format, tc.norm, tc.ton, tc.npi)
		}
	}
}

func TestBuildSubmitSMAutoDetectsAddressTONNPI(t *testing.T) {
	p := BuildSubmitSM(1, SubmitSMRequest{Source: Address{Addr: "ACME"}, Destination: Address{Addr: "+977 980-000-0000"}, ShortMessage: []byte("hello")})
	req, err := ParseSubmitSM(p)
	if err != nil {
		t.Fatal(err)
	}
	if req.Source.TON != TONAlphanumeric || req.Source.NPI != NPIUnknown || req.Source.Addr != "ACME" {
		t.Fatalf("source not auto detected: %+v", req.Source)
	}
	if req.Destination.TON != TONInternational || req.Destination.NPI != NPIISDN || req.Destination.Addr != "9779800000000" {
		t.Fatalf("destination not auto detected: %+v", req.Destination)
	}
}

func TestExplicitTONNPIPreservedWhileAddressIsNormalized(t *testing.T) {
	a := AutoDetectAddress(Address{TON: TONNational, NPI: NPIPrivate, Addr: "+977-9800000000"})
	if a.TON != TONNational || a.NPI != NPIPrivate || a.Addr != "9779800000000" {
		t.Fatalf("explicit TON/NPI should be preserved and address normalized: %+v", a)
	}
}

func TestSubmitMultiAutoDetectsEachDestination(t *testing.T) {
	p := BuildSubmitMulti(2, SubmitMultiRequest{
		SubmitSMRequest: SubmitSMRequest{Source: Address{Addr: "INFO"}, ShortMessage: []byte("bulk")},
		Destinations:    []Address{{Addr: "+9779800000000"}, {Addr: "12345"}},
	})
	req, err := ParseSubmitMulti(p)
	if err != nil {
		t.Fatal(err)
	}
	if req.Source.TON != TONAlphanumeric || req.Destinations[0].TON != TONInternational || req.Destinations[1].TON != TONAbbreviated {
		t.Fatalf("submit_multi addresses not detected: source=%+v dests=%+v", req.Source, req.Destinations)
	}
}
