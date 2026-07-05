package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/smppflow/pkg/alerts"
	"github.com/oarkflow/smppflow/pkg/billing"
	"github.com/oarkflow/smppflow/pkg/contacts"
	"github.com/oarkflow/smppflow/pkg/invoice"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/optout"
	"github.com/oarkflow/smppflow/pkg/ratelimit"
)

func main() {
	ctx := context.Background()

	limiter := ratelimit.New()
	_ = limiter.SetPolicy(ratelimit.Policy{Key: "tenant:t1", Scope: ratelimit.ScopeTenant, RatePerSecond: 10, Burst: 20, QueueLimit: 100, Adaptive: true, MinRatePerSecond: 1, MaxRatePerSecond: 50})
	decision, _ := limiter.Allow(ctx, "tenant:t1", 1)
	fmt.Println("rate_allowed", decision.Allowed)

	optStore := optout.NewStore()
	reply, _ := optout.NewProcessor(optStore).ProcessInbound(ctx, "t1", "+9779800000000", "STOP", "deliver_sm")
	fmt.Println("keyword_action", reply.Action, "blocked", optStore.Has(ctx, "t1", "9779800000000"))

	contactStore := contacts.NewStore()
	_, _ = contacts.ImportCSV(ctx, contactStore, "t1", strings.NewReader("msisdn,first_name,country,tags\n+9771,A,NP,vip\n+9772,B,NP,promo\n"))
	fmt.Println("vip_contacts", len(contactStore.Select(ctx, contacts.Segment{TenantID: "t1", IncludeTags: []string{"vip"}})))

	alertEngine := alerts.New()
	alertEngine.AddRule(alerts.Rule{ID: "dlq-depth", Name: "DLQ depth high", Signal: "dlq_depth", Above: 10, Severity: alerts.Critical})
	incidents := alertEngine.Evaluate(ctx, alerts.Signal{Name: "dlq_depth", Value: 12, At: time.Now()})
	fmt.Println("open_incidents", len(incidents))

	now := time.Now()
	rated := []billing.RatedMessage{{MessageID: message.ID("m1"), TenantID: "t1", ProviderID: "p1", Country: "NP", Segments: 2, Sell: billing.Money{Currency: "USD", AmountMicros: 200000}, RatedAt: now}}
	inv, _ := invoice.Build("t1", now.Add(-time.Hour), now.Add(time.Hour), rated, 0.0)
	fmt.Println("invoice_total_micros", inv.TotalMicros)
}
