package provider_test

import (
	"testing"

	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/provider"
)

func TestGeneric34Defaults(t *testing.T) {
	p := provider.Generic34("prov", "127.0.0.1:2775", "sys", "pw")
	if p.ID != "prov" || p.Name != "prov" {
		t.Fatalf("id/name=%q/%q", p.ID, p.Name)
	}
	if p.Version != protocol.Version34 {
		t.Fatalf("version=%v want 34", p.Version)
	}
	if p.BindMode != protocol.BindModeTransceiver {
		t.Fatalf("bind mode=%v", p.BindMode)
	}
	if p.SourceTON != protocol.TONAlphanumeric || p.SourceNPI != protocol.NPIUnknown {
		t.Fatalf("source ton/npi=%v/%v", p.SourceTON, p.SourceNPI)
	}
	if p.DestTON != protocol.TONInternational || p.DestNPI != protocol.NPIISDN {
		t.Fatalf("dest ton/npi=%v/%v", p.DestTON, p.DestNPI)
	}
	if p.TPS != 100 || p.WindowSize != 16 {
		t.Fatalf("tps/window=%d/%d", p.TPS, p.WindowSize)
	}
	if p.EnquireInterval <= 0 || p.ResponseTimeout <= 0 {
		t.Fatalf("intervals must be positive: %v/%v", p.EnquireInterval, p.ResponseTimeout)
	}
	if p.Multipart != provider.MultipartUDH || p.DLR != provider.DLRMixed {
		t.Fatalf("multipart/dlr=%v/%v", p.Multipart, p.DLR)
	}
	for _, ok := range []bool{p.SupportsQuery, p.SupportsCancel, p.SupportsReplace, p.SupportsDataSM, p.SupportsSubmitMulti} {
		if !ok {
			t.Fatal("Generic34 must enable all optional operations")
		}
	}
	if p.UseTLS {
		t.Fatal("Generic34 must not enable TLS by default")
	}
}

func TestLegacy33Profile(t *testing.T) {
	p := provider.Legacy33("old", "127.0.0.1:2775", "sys", "pw")
	if p.Version != protocol.Version33 {
		t.Fatalf("version=%v want 33", p.Version)
	}
	if p.SupportsDataSM || p.SupportsSubmitMulti {
		t.Fatal("SMPP 3.3 must not advertise data_sm or submit_multi")
	}
	if p.Multipart != provider.MultipartUDH {
		t.Fatalf("multipart=%v want UDH", p.Multipart)
	}
}

func TestModern50Profile(t *testing.T) {
	p := provider.Modern50("new", "127.0.0.1:2775", "sys", "pw")
	if p.Version != protocol.Version50 {
		t.Fatalf("version=%v want 50", p.Version)
	}
	if p.Multipart != provider.MultipartSAR {
		t.Fatalf("multipart=%v want SAR", p.Multipart)
	}
}

func TestBindRequestDefaults(t *testing.T) {
	p := provider.Profile{SystemID: "sys", Password: "pw"}
	req := p.BindRequest()
	if req.Mode != protocol.BindModeTransceiver {
		t.Fatalf("mode=%v want transceiver", req.Mode)
	}
	if req.InterfaceVersion != protocol.Version34 {
		t.Fatalf("version=%v want 34", req.InterfaceVersion)
	}
	if req.SystemID != "sys" || req.Password != "pw" {
		t.Fatalf("credentials not carried: %+v", req)
	}
	p = provider.Profile{BindMode: protocol.BindModeTransmitter, Version: protocol.Version50}
	req = p.BindRequest()
	if req.Mode != protocol.BindModeTransmitter || req.InterfaceVersion != protocol.Version50 {
		t.Fatalf("explicit values must win: %+v", req)
	}
}

func TestNewCertificationProfileCapabilities(t *testing.T) {
	p := provider.Generic34("prov", "addr", "sys", "pw")
	p.UseTLS = true
	p.Multipart = provider.MultipartSAR
	cp := provider.NewCertificationProfile(p)
	got := map[provider.Capability]bool{}
	for _, c := range cp.Capabilities {
		got[c] = true
	}
	for _, want := range []provider.Capability{provider.CapDLR, provider.CapMO, provider.CapUDH, provider.CapQuerySM, provider.CapTLS, provider.CapSAR} {
		if !got[want] {
			t.Fatalf("missing capability %q in %v", want, cp.Capabilities)
		}
	}
	if cp.TONNPI["source"] != "5:0" {
		t.Fatalf("source tonnpi=%q want 5:0", cp.TONNPI["source"])
	}
	if cp.TONNPI["destination"] != "1:1" {
		t.Fatalf("destination tonnpi=%q want 1:1", cp.TONNPI["destination"])
	}
	if len(cp.ErrorRules) == 0 || len(cp.DLRExamples) == 0 {
		t.Fatal("certification profile must carry error rules and DLR examples")
	}
	if cp.Version != "1.0" {
		t.Fatalf("version=%q want 1.0", cp.Version)
	}

	p2 := provider.Legacy33("prov", "addr", "sys", "pw")
	cp2 := provider.NewCertificationProfile(p2)
	got2 := map[provider.Capability]bool{}
	for _, c := range cp2.Capabilities {
		got2[c] = true
	}
	if !got2[provider.CapQuerySM] {
		t.Fatal("Legacy33 keeps SupportsQuery, so the query capability must be present")
	}
	if got2[provider.CapTLS] || got2[provider.CapSAR] {
		t.Fatal("Legacy33 without TLS/SAR must not advertise those capabilities")
	}
}

func TestWithVendorQuirks(t *testing.T) {
	base := provider.Generic34("prov", "addr", "sys", "pw")
	base.Meta = map[string]string{"keep": "me"}

	syn := provider.WithVendorQuirks(base, "Syniverse")
	if syn.DLR != provider.DLRMixed || syn.MessageIDFormat != "hex" {
		t.Fatalf("syniverse quirks not applied: %+v", syn)
	}
	if syn.Meta["vendor"] != "Syniverse" || syn.Meta["keep"] != "me" {
		t.Fatalf("meta=%v", syn.Meta)
	}

	tw := provider.WithVendorQuirks(base, "twilio")
	if tw.DLR != provider.DLRTLV || tw.SupportsQuery {
		t.Fatalf("twilio quirks not applied: %+v", tw)
	}

	inf := provider.WithVendorQuirks(base, "infobip")
	if inf.Multipart != provider.MultipartSAR || !inf.SupportsDataSM {
		t.Fatalf("infobip quirks not applied: %+v", inf)
	}

	tls := provider.WithVendorQuirks(base, "generic-tls")
	if !tls.UseTLS {
		t.Fatal("generic-tls quirk must enable TLS")
	}

	plain := provider.WithVendorQuirks(provider.Generic34("p", "a", "s", "pw"), "someone-else")
	if plain.Meta["vendor"] != "someone-else" {
		t.Fatalf("meta=%v", plain.Meta)
	}
}
