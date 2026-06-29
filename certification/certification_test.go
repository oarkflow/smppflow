package certification

import (
	"context"
	"testing"

	"github.com/oarkflow/smppflow/message"
	"github.com/oarkflow/smppflow/protocol"
)

func TestValidateSubmitTLVRequirement(t *testing.T) {
	p := GenericPack("p1")
	p.TLVQuirks = []TLVQuirk{{Tag: protocol.TLVReceiptedMessageID, Required: true}}
	rep := ValidateSubmit(context.Background(), p, message.SubmitRequest{From: "ACME", To: "+16502530000", Text: "hello"}, nil)
	if rep.Passed || len(rep.Findings) == 0 {
		t.Fatalf("expected missing TLV failure: %+v", rep)
	}
	rep = ValidateSubmit(context.Background(), p, message.SubmitRequest{From: "ACME", To: "+16502530000", Text: "hello"}, []protocol.TLV{protocol.TLVCString(protocol.TLVReceiptedMessageID, "id")})
	if !rep.Passed {
		t.Fatalf("expected pass: %+v", rep)
	}
}
