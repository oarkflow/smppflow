package message_test

import (
	"testing"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/protocol"
)

func TestParseDLRFromText(t *testing.T) {
	raw := "id:smsc-000001 sub:001 dlvrd:001 submit date:2610031807 done date:2610031807 stat:DELIVRD err:000 text:ok"
	d := message.ParseDLR(raw, nil)
	if d.ProviderMessageID != "smsc-000001" {
		t.Fatalf("ProviderMessageID=%q", d.ProviderMessageID)
	}
	if d.State != lifecycle.MsgDelivered {
		t.Fatalf("State=%q want delivered", d.State)
	}
	if d.ErrorCode != "000" {
		t.Fatalf("ErrorCode=%q", d.ErrorCode)
	}
	if d.Raw != raw || d.DoneAt.IsZero() {
		t.Fatalf("receipt=%+v", d)
	}
}

func TestParseDLRStateFromTextVariants(t *testing.T) {
	cases := map[string]lifecycle.MessageState{
		"stat:EXPIRED":     lifecycle.MsgExpired,
		"stat:UNDELIV":     lifecycle.MsgUndeliverable,
		"stat:REJECTD":     lifecycle.MsgRejected,
		"stat:ACCEPTD":     lifecycle.MsgAccepted,
		"stat:ENROUTE":     lifecycle.MsgAccepted,
		"stat:UNKNOWN_XYZ": lifecycle.MsgUnknown,
	}
	for raw, want := range cases {
		d := message.ParseDLR("id:x "+raw+" err:0", nil)
		if d.State != want {
			t.Fatalf("%s: State=%q want %q", raw, d.State, want)
		}
	}
}

func TestParseDLRFromTLVs(t *testing.T) {
	tlvs := []protocol.TLV{
		protocol.TLVCString(protocol.TLVReceiptedMessageID, "tlv-id"),
		protocol.TLVByte(protocol.TLVMessageState, 2),
	}
	d := message.ParseDLR("no regex match here", tlvs)
	if d.ProviderMessageID != "tlv-id" {
		t.Fatalf("ProviderMessageID=%q", d.ProviderMessageID)
	}
	if d.State != lifecycle.MsgDelivered {
		t.Fatalf("State=%q want delivered (TLV message_state=2)", d.State)
	}
}

func TestParseDLRTLVsTakePrecedence(t *testing.T) {
	tlvs := []protocol.TLV{
		protocol.TLVCString(protocol.TLVReceiptedMessageID, "tlv-id"),
		protocol.TLVByte(protocol.TLVMessageState, 8),
	}
	raw := "id:text-id sub:001 dlvrd:001 stat:DELIVRD err:042"
	d := message.ParseDLR(raw, tlvs)
	if d.ProviderMessageID != "tlv-id" {
		t.Fatalf("ProviderMessageID=%q want tlv-id", d.ProviderMessageID)
	}
	if d.State != lifecycle.MsgRejected {
		t.Fatalf("State=%q want rejected (TLV state=8)", d.State)
	}
	if d.ErrorCode != "042" {
		t.Fatalf("ErrorCode=%q want 042 from text", d.ErrorCode)
	}
}

func TestParseDLRTLVStateMapping(t *testing.T) {
	cases := map[byte]lifecycle.MessageState{
		1: lifecycle.MsgAccepted,
		2: lifecycle.MsgDelivered,
		3: lifecycle.MsgExpired,
		4: lifecycle.MsgUndeliverable,
		5: lifecycle.MsgAccepted,
		6: lifecycle.MsgUndeliverable,
		7: lifecycle.MsgUnknown,
		8: lifecycle.MsgRejected,
		9: lifecycle.MsgUnknown,
	}
	for b, want := range cases {
		d := message.ParseDLR("", []protocol.TLV{protocol.TLVByte(protocol.TLVMessageState, b)})
		if d.State != want {
			t.Fatalf("tlv state %d: State=%q want %q", b, d.State, want)
		}
	}
}

func TestParseDLRWithoutPayload(t *testing.T) {
	d := message.ParseDLR("", nil)
	if d.State != lifecycle.MsgUnknown || d.ProviderMessageID != "" || d.ErrorCode != "" {
		t.Fatalf("empty payload should yield unknown receipt, got %+v", d)
	}
}
