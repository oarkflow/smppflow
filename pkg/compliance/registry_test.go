package compliance

import (
	"context"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
)

type senderOK bool

func (s senderOK) IsSenderApproved(ctx context.Context, tenant, sender, country string) bool {
	return bool(s)
}

type templateOK bool

func (t templateOK) IsTemplateApproved(ctx context.Context, tenant, id, country string) bool {
	return bool(t)
}
func TestRegistryEvaluate(t *testing.T) {
	r := NewRegistry()
	r.SetNow(func() time.Time { return time.Date(2026, 1, 1, 23, 0, 0, 0, time.UTC) })
	r.SetSenderLookup(senderOK(true))
	r.SetTemplateLookup(templateOK(false))
	_ = r.UpsertRule(CountryRule{Country: "NP", RequiresSenderApproval: true, RequiresTemplate: true, QuietHours: QuietHours{StartHour: 22, EndHour: 6, Timezone: "UTC"}, BlockedTerms: []string{"spam"}})
	dec := r.Evaluate(context.Background(), message.SubmitRequest{TenantID: "t1", From: "SENDER", To: "9779800000000", Text: "hello spam", TemplateID: "tpl"}, "NP")
	if dec.Allowed {
		t.Fatalf("expected blocked: %+v", dec)
	}
	if len(dec.Reasons) < 3 {
		t.Fatalf("expected multiple reasons: %+v", dec.Reasons)
	}
}
