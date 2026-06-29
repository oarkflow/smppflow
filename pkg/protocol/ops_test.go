package protocol

import "testing"

func TestAdvancedBuilders(t *testing.T) {
	src := Address{TON: TONAlphanumeric, NPI: NPIUnknown, Addr: "SRC"}
	dst := Address{TON: TONInternational, NPI: NPIISDN, Addr: "9779800000000"}
	q := BuildQuerySM(1, QuerySMRequest{MessageID: "abc", Source: src})
	qr, err := ParseQuerySM(q)
	if err != nil || qr.MessageID != "abc" || qr.Source.Addr != "SRC" {
		t.Fatalf("query parse %+v %v", qr, err)
	}
	c := BuildCancelSM(2, CancelSMRequest{MessageID: "abc", Source: src, Destination: dst})
	cr, err := ParseCancelSM(c)
	if err != nil || cr.Destination.Addr == "" {
		t.Fatalf("cancel parse %+v %v", cr, err)
	}
	r := BuildReplaceSM(3, ReplaceSMRequest{MessageID: "abc", Source: src, ShortMessage: []byte("new")})
	rr, err := ParseReplaceSM(r)
	if err != nil || string(rr.ShortMessage) != "new" {
		t.Fatalf("replace parse %+v %v", rr, err)
	}
	if BuildDataSM(4, SubmitSMRequest{Source: src, Destination: dst}).CommandID != DataSM {
		t.Fatal("data_sm command")
	}
	if BuildSubmitMulti(5, SubmitMultiRequest{SubmitSMRequest: SubmitSMRequest{Source: src, Destination: dst}, Destinations: []Address{dst}}).CommandID != SubmitMulti {
		t.Fatal("submit_multi command")
	}
}
