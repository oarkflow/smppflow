package protocol_test

import (
	"testing"

	"github.com/oarkflow/smppflow/codec"
	"github.com/oarkflow/smppflow/protocol"
)

func TestBindRoundTrip(t *testing.T) {
	p := protocol.BuildBind(7, protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "sys", Password: "pw", InterfaceVersion: protocol.Version34})
	raw, err := codec.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := codec.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	b, err := protocol.ParseBind(got)
	if err != nil {
		t.Fatal(err)
	}
	if b.SystemID != "sys" || b.Password != "pw" || b.InterfaceVersion != protocol.Version34 {
		t.Fatalf("bad bind: %+v", b)
	}
}
func TestSubmitRoundTrip(t *testing.T) {
	p := protocol.BuildSubmitSM(1, protocol.SubmitSMRequest{Source: protocol.Address{Addr: "A"}, Destination: protocol.Address{Addr: "977"}, ShortMessage: []byte("hello"), RegisteredDelivery: 1, TLVs: []protocol.TLV{protocol.TLVByte(protocol.TLVMessageState, 2)}})
	raw, _ := codec.Encode(p)
	got, err := codec.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	s, err := protocol.ParseSubmitSM(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(s.ShortMessage) != "hello" || len(s.TLVs) != 1 {
		t.Fatalf("bad submit: %+v", s)
	}
}
