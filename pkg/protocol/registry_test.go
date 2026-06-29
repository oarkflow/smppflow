package protocol

import "testing"

func TestCapabilitiesAndValidation(t *testing.T) {
	if Capabilities(Version33).SupportsTransceiver {
		t.Fatal("3.3 must not support trx")
	}
	if !Capabilities(Version50).SupportsCellBroadcast {
		t.Fatal("5.0 must support cell broadcast")
	}
	if SupportsCommand(Version34, BroadcastSM) {
		t.Fatal("3.4 must not support broadcast_sm")
	}
	if err := ValidatePDUForVersion(Version50, PDU{CommandID: BroadcastSM, TLVs: []TLV{TLVByte(TLVCongestionState, 50)}}); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePDUForVersion(Version34, PDU{CommandID: BroadcastSM}); err == nil {
		t.Fatal("expected broadcast_sm reject on 3.4")
	}
	if got := NegotiateVersion(Version50, Version34, Version33); got != Version34 {
		t.Fatalf("expected 3.4 fallback, got %s", got)
	}
}
