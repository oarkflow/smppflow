package compliance

import (
	"context"
	"testing"

	"github.com/oarkflow/smppflow/message"
)

func TestComplianceBlocksBadContent(t *testing.T) {
	e := New()
	_ = e.AddBlockedPattern(`(?i)free money`)
	e.VerifySender(SenderPolicy{TenantID: "t", Sender: "ACME", Verified: true, AllowedCountries: []string{"US"}})
	d := e.Check(context.Background(), message.SubmitRequest{TenantID: "t", From: "ACME", To: "+1", Text: "get free money now"}, "US")
	if d.Allowed {
		t.Fatalf("expected block: %+v", d)
	}
	reply, handled := e.HandleInbound(context.Background(), "+1", "STOP")
	if !handled || reply == "" {
		t.Fatalf("STOP not handled")
	}
}
