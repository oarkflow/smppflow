package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/smppflow/pkg/compliance"
	"github.com/oarkflow/smppflow/pkg/intelligence"
	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/rbac"
	"github.com/oarkflow/smppflow/pkg/reports"
	"github.com/oarkflow/smppflow/pkg/storage"
)

func main() {
	ctx := context.Background()

	lookup := intelligence.NewLookupEngine(intelligence.LookupPolicy{RequireReachable: true}, intelligence.StaticAdapter{Profiles: map[string]intelligence.NumberProfile{
		"9779800000000": {Country: "NP", MCC: "429", MNC: "01", Carrier: "demo", Reachability: intelligence.ReachabilityReachable},
	}})
	profile, _ := lookup.Lookup(ctx, "9779800000000")
	fmt.Println("lookup allowed", profile.Allowed, profile.Profile.Country)

	registry := compliance.NewRegistry()
	_ = registry.UpsertRule(compliance.CountryRule{Country: "NP", QuietHours: compliance.QuietHours{StartHour: 22, EndHour: 6, Timezone: "UTC"}})
	fmt.Println("compliance allowed", registry.Evaluate(ctx, message.SubmitRequest{TenantID: "t1", From: "SENDER", To: "9779800000000", Text: "hello"}, "NP").Allowed)

	policy := rbac.NewPolicy()
	keys := rbac.NewKeyStore()
	_, secret, _ := keys.Create("t1", "demo", []string{"message:read"}, time.Hour)
	principal, _ := keys.Authenticate(secret)
	fmt.Println("rbac allowed", policy.Check(principal, "t1", "message", "read").Allowed)

	st := storage.NewMemory()
	_ = st.SaveMessage(ctx, &message.Message{ID: "m1", TenantID: "t1", ProviderID: "p1", State: lifecycle.MsgDelivered, CreatedAt: time.Now(), Parts: []message.SegmentRecord{{ID: "s1"}}})
	sink := &reports.MemorySink{}
	sch := reports.NewScheduler(st)
	sch.RegisterSink("mem", sink)
	_ = sch.Upsert(reports.ScheduledReport{ID: "delivery", Name: "delivery", Format: reports.ExportJSON, SinkName: "mem", Enabled: true})
	fmt.Println("reports", len(sch.RunDue(ctx)), len(sink.Objects))
}
