package regulation

import (
	"context"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/message"
)

func TestRegulationBlocksUnregisteredSender(t *testing.T) {
	db := New()
	db.SetRule(CountryRule{Country: "US", SenderRegistrationRequired: true, RequiredFooter: []string{"STOP"}})
	dec := db.Check(context.Background(), message.SubmitRequest{From: "ACME", To: "+1", Text: "hello STOP"}, "US", time.Now())
	if dec.Allowed {
		t.Fatalf("expected block")
	}
	db.RegisterSender("US", "ACME")
	dec = db.Check(context.Background(), message.SubmitRequest{From: "ACME", To: "+1", Text: "hello STOP"}, "US", time.Now())
	if !dec.Allowed {
		t.Fatalf("expected allow: %+v", dec)
	}
}
