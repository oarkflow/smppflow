package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/smppflow/message"
	"github.com/oarkflow/smppflow/provider"
	"github.com/oarkflow/smppflow/routing"
)

func main() {
	ctx := context.Background()
	r := routing.New()
	_ = r.RegisterProvider(provider.Generic34("np-premium", "127.0.0.1:2775", "sys", "secret"))
	_ = r.RegisterProvider(provider.Generic34("np-economy", "127.0.0.1:2776", "sys", "secret"))
	_ = r.RegisterProvider(provider.Modern50("global-fast", "127.0.0.1:2777", "sys", "secret"))
	r.UpdateProviderStats(routing.ProviderStats{ProviderID: "np-premium", QualityScore: 96, DeliveryRate: 0.995, AvgLatency: 80 * time.Millisecond, MaxTPS: 1000, CurrentTPS: 20, CostPerSegment: 0.015})
	r.UpdateProviderStats(routing.ProviderStats{ProviderID: "np-economy", QualityScore: 82, DeliveryRate: 0.970, AvgLatency: 300 * time.Millisecond, MaxTPS: 2000, CurrentTPS: 100, CostPerSegment: 0.006})
	r.UpdateProviderStats(routing.ProviderStats{ProviderID: "global-fast", QualityScore: 91, DeliveryRate: 0.985, AvgLatency: 60 * time.Millisecond, MaxTPS: 5000, CurrentTPS: 400, CostPerSegment: 0.020})

	_ = r.RegisterRule(routing.Rule{ID: "vip-user", TenantID: "tenant-a", UserID: "user-vip", Prefix: "977", ProviderID: "np-premium", Priority: 1, Weight: 30, MinQuality: 90})
	_ = r.RegisterRule(routing.Rule{ID: "promo-sender-economy", TenantID: "tenant-a", Sender: "PROMO", Prefix: "977", ProviderID: "np-economy", Priority: 5, Weight: 80, MaxCost: 0.01})
	_ = r.RegisterRule(routing.Rule{ID: "global-quality", TenantID: "tenant-a", ProviderID: "global-fast", Priority: 20, Weight: 5, MinQuality: 85})

	cases := []message.SubmitRequest{
		{TenantID: "tenant-a", From: "OARK", To: "9779800000000", Text: "VIP", Meta: map[string]string{"user_id": "user-vip"}},
		{TenantID: "tenant-a", From: "PROMO", To: "9779811111111", Text: "Campaign", Meta: map[string]string{"user_id": "user-normal"}},
		{TenantID: "tenant-a", From: "OTP", To: "14155550100", Text: "Global", Meta: map[string]string{"user_id": "user-normal"}},
	}
	for _, req := range cases {
		d, err := r.Route(ctx, req)
		if err != nil {
			panic(err)
		}
		fmt.Printf("to=%s sender=%s user=%s provider=%s score=%.2f rule=%s reason=%s\n", req.To, req.From, req.Meta["user_id"], d.Provider.ID, d.Score, d.Rule.ID, d.Reason)
	}
}
