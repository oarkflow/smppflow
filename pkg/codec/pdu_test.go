package codec_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/oarkflow/smppflow/pkg/codec"
	"github.com/oarkflow/smppflow/pkg/protocol"
)

func samplePDU() protocol.PDU {
	return protocol.PDU{
		CommandID:      protocol.SubmitSM,
		CommandStatus:  protocol.ESME_ROK,
		SequenceNumber: 42,
		Body:           []byte("body-bytes"),
		TLVs: []protocol.TLV{
			{Tag: protocol.TLVReceiptedMessageID, Value: []byte("msg-1")},
			{Tag: protocol.TLVMessageState, Value: []byte{2}},
		},
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	p := samplePDU()
	raw, err := codec.Encode(p)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := codec.Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.CommandID != p.CommandID || got.CommandStatus != p.CommandStatus || got.SequenceNumber != p.SequenceNumber {
		t.Fatalf("header mismatch: %+v", got)
	}
	if !bytes.Equal(got.Body, append(append([]byte(nil), p.Body...), tlvBytes(p.TLVs)...)) {
		t.Fatalf("body mismatch: %x", got.Body)
	}
}

func tlvBytes(tlvs []protocol.TLV) []byte {
	var out []byte
	for _, t := range tlvs {
		out = append(out, byte(t.Tag>>8), byte(t.Tag), byte(len(t.Value)>>8), byte(len(t.Value)))
		out = append(out, t.Value...)
	}
	return out
}

func TestEncodeRejectsOversize(t *testing.T) {
	p := protocol.PDU{CommandID: protocol.SubmitSM, Body: make([]byte, codec.MaxPDULength)}
	if _, err := codec.Encode(p); err == nil {
		t.Fatal("oversize PDU must be rejected")
	}
	p = protocol.PDU{CommandID: protocol.SubmitSM, TLVs: []protocol.TLV{{Tag: 1, Value: make([]byte, 65536)}}}
	if _, err := codec.Encode(p); err == nil || !strings.Contains(err.Error(), "tlv") {
		t.Fatalf("oversize TLV must be rejected, got %v", err)
	}
}

func TestDecodeErrors(t *testing.T) {
	if _, err := codec.Decode([]byte{0, 0, 0, 1}); err == nil {
		t.Fatal("short header must be rejected")
	}
	raw, err := codec.Encode(samplePDU())
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), raw...)
	bad[3] = byte(len(raw) + 1)
	if _, err := codec.Decode(bad); err == nil || !strings.Contains(err.Error(), "length mismatch") {
		t.Fatalf("length mismatch must be rejected, got %v", err)
	}
	bad = append([]byte(nil), raw...)
	bad[0], bad[1], bad[2], bad[3] = 0, 0, 0, 8
	if _, err := codec.Decode(bad); err == nil {
		t.Fatal("length below header size must be rejected")
	}
	bad = append([]byte(nil), raw...)
	bad[0], bad[1], bad[2], bad[3] = 0xFF, 0xFF, 0xFF, 0xFF
	if _, err := codec.Decode(bad); err == nil {
		t.Fatal("length above MaxPDULength must be rejected")
	}
}

func TestWriteReadPDU(t *testing.T) {
	p := samplePDU()
	var buf bytes.Buffer
	if err := codec.WritePDU(&buf, p); err != nil {
		t.Fatalf("WritePDU: %v", err)
	}
	got, err := codec.ReadPDU(&buf)
	if err != nil {
		t.Fatalf("ReadPDU: %v", err)
	}
	if got.SequenceNumber != p.SequenceNumber || got.CommandID != p.CommandID {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}

	if _, err := codec.ReadPDU(bytes.NewReader(nil)); err == nil {
		t.Fatal("ReadPDU on empty reader must fail")
	}
	raw, _ := codec.Encode(p)
	if _, err := codec.ReadPDU(bytes.NewReader(raw[:10])); err == nil {
		t.Fatal("ReadPDU with truncated body must fail")
	}
	short := []byte{0, 0, 0, 32}
	if _, err := codec.ReadPDU(bytes.NewReader(short)); err == nil {
		t.Fatal("ReadPDU with invalid header length must fail")
	}
}

func TestSplitTLV(t *testing.T) {
	body := append([]byte("mand"), tlvBytes([]protocol.TLV{
		{Tag: 0x0001, Value: []byte("a")},
		{Tag: 0x0427, Value: []byte{7}},
	})...)
	mandatory, tlvs, err := codec.SplitTLV(body, 4)
	if err != nil {
		t.Fatalf("SplitTLV: %v", err)
	}
	if string(mandatory) != "mand" {
		t.Fatalf("mandatory=%q", mandatory)
	}
	if len(tlvs) != 2 {
		t.Fatalf("tlvs=%d want 2", len(tlvs))
	}
	if tlvs[0].Tag != 0x0001 || string(tlvs[0].Value) != "a" {
		t.Fatalf("tlv0=%+v", tlvs[0])
	}
	if tlvs[1].Tag != 0x0427 || !bytes.Equal(tlvs[1].Value, []byte{7}) {
		t.Fatalf("tlv1=%+v", tlvs[1])
	}

	if _, _, err := codec.SplitTLV(body, -1); err == nil {
		t.Fatal("negative offset must fail")
	}
	if _, _, err := codec.SplitTLV(body, len(body)+1); err == nil {
		t.Fatal("offset past end must fail")
	}
	if _, _, err := codec.SplitTLV(append([]byte("mand"), 0x00), 4); err == nil {
		t.Fatal("truncated tlv header must fail")
	}
	if _, _, err := codec.SplitTLV(append([]byte("mand"), 0x00, 0x01, 0x00, 0x05, 'x'), 4); err == nil {
		t.Fatal("truncated tlv value must fail")
	}
}
