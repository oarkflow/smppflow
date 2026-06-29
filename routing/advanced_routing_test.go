package routing

import (
	"context"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/message"
	"github.com/oarkflow/smppflow/provider"
)

func TestAdvancedRouteByUserSenderPrefixAndQuality(t *testing.T) {
	ctx := context.Background()
	r := New()
	_ = r.RegisterProvider(provider.Generic34("premium", "x", "s", "p"))
	_ = r.RegisterProvider(provider.Generic34("economy", "x", "s", "p"))
	_ = r.RegisterProvider(provider.Generic34("global", "x", "s", "p"))
	r.UpdateProviderStats(ProviderStats{ProviderID: "premium", QualityScore: 95, DeliveryRate: 0.99, AvgLatency: 50 * time.Millisecond, MaxTPS: 1000, CostPerSegment: 0.02})
	r.UpdateProviderStats(ProviderStats{ProviderID: "economy", QualityScore: 81, DeliveryRate: 0.97, AvgLatency: 250 * time.Millisecond, MaxTPS: 3000, CostPerSegment: 0.005})
	r.UpdateProviderStats(ProviderStats{ProviderID: "global", QualityScore: 90, DeliveryRate: 0.98, AvgLatency: 60 * time.Millisecond, MaxTPS: 5000, CostPerSegment: 0.015})
	_ = r.RegisterRule(Rule{ID: "vip", TenantID: "t1", UserID: "vip", Prefix: "977", ProviderID: "premium", Priority: 1, Weight: 20, MinQuality: 90})
	_ = r.RegisterRule(Rule{ID: "promo", TenantID: "t1", Sender: "PROMO", Prefix: "977", ProviderID: "economy", Priority: 2, Weight: 20, MaxCost: 0.01})
	_ = r.RegisterRule(Rule{ID: "fallback", TenantID: "t1", ProviderID: "global", Priority: 100})
	d, err := r.Route(ctx, message.SubmitRequest{TenantID: "t1", From: "OARK", To: "9779800000000", Meta: map[string]string{"user_id": "vip"}})
	if err != nil {
		t.Fatal(err)
	}
	if d.Provider.ID != "premium" {
		t.Fatalf("want premium got %s", d.Provider.ID)
	}
	d, err = r.Route(ctx, message.SubmitRequest{TenantID: "t1", From: "PROMO", To: "9779800000001", Meta: map[string]string{"user_id": "normal"}})
	if err != nil {
		t.Fatal(err)
	}
	if d.Provider.ID != "economy" {
		t.Fatalf("want economy got %s", d.Provider.ID)
	}
	d, err = r.Route(ctx, message.SubmitRequest{TenantID: "t1", From: "OTP", To: "14155550100", Meta: map[string]string{"user_id": "normal"}})
	if err != nil {
		t.Fatal(err)
	}
	if d.Provider.ID != "global" {
		t.Fatalf("want global got %s", d.Provider.ID)
	}
}

func TestRouterNormalizesSenderAndRecipientForRules(t *testing.T) {
	r := New()
	if err := r.RegisterProvider(provider.Profile{ID: "p1", Name: "P1", TPS: 100}); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterRule(Rule{ID: "np", TenantID: "t1", Sender: "BRAND", Prefix: "+977", ProviderID: "p1", Priority: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	dec, err := r.Route(context.Background(), message.SubmitRequest{TenantID: "t1", From: " BRAND ", To: "+977 980-000-0000"})
	if err != nil {
		t.Fatal(err)
	}
	if dec.Rule.ID != "np" {
		t.Fatalf("expected normalized route np, got %+v", dec.Rule)
	}
}
