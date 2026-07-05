package routing

import (
	"context"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/provider"
)

func TestRouteByUserSenderPrefixAndQuality(t *testing.T) {
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

func TestSingleRouterStrategies(t *testing.T) {
	ctx := context.Background()
	r := New()
	r.SetStrategy(StrategyLeastCost)
	_ = r.RegisterProvider(provider.Generic34("cheap", "", "", ""))
	_ = r.RegisterProvider(provider.Generic34("fast", "", "", ""))
	r.UpdateProviderStats(ProviderStats{ProviderID: "cheap", QualityScore: 80, CostPerSegment: .001, AvgLatency: 100 * time.Millisecond, MaxTPS: 100})
	r.UpdateProviderStats(ProviderStats{ProviderID: "fast", QualityScore: 95, CostPerSegment: .02, AvgLatency: 10 * time.Millisecond, MaxTPS: 100})
	_ = r.RegisterRule(Rule{ID: "cheap", Prefix: "977", ProviderID: "cheap", Priority: 1})
	_ = r.RegisterRule(Rule{ID: "fast", Prefix: "977", ProviderID: "fast", Priority: 1})
	d, err := r.Route(ctx, message.SubmitRequest{From: "A", To: "+9779841000000"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Provider.ID != "cheap" {
		t.Fatalf("want cheap got %s", d.Provider.ID)
	}
	r.SetStrategy(StrategyFastest)
	d, err = r.Route(ctx, message.SubmitRequest{From: "A", To: "+9779841000000"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Provider.ID != "fast" {
		t.Fatalf("want fast got %s", d.Provider.ID)
	}
}

func TestTenantFallbackIsSafe(t *testing.T) {
	r := New()
	_ = r.RegisterProvider(provider.Generic34("p1", "", "", ""))
	_ = r.RegisterProvider(provider.Generic34("p2", "", "", ""))
	_ = r.RegisterRule(Rule{ID: "t2", TenantID: "tenant-b", ProviderID: "p2", Priority: 1})
	_, err := r.Route(context.Background(), message.SubmitRequest{TenantID: "tenant-a", To: "9771"})
	if err == nil {
		t.Fatal("expected no tenant-a route instead of global provider fallback")
	}
}

func TestStrictPriorityBeforeScore(t *testing.T) {
	r := New()
	r.SetStrategy(StrategyBestQuality)
	_ = r.RegisterProvider(provider.Generic34("primary", "", "", ""))
	_ = r.RegisterProvider(provider.Generic34("secondary", "", "", ""))
	r.UpdateProviderStats(ProviderStats{ProviderID: "primary", QualityScore: 50, DeliveryRate: .9, MaxTPS: 100})
	r.UpdateProviderStats(ProviderStats{ProviderID: "secondary", QualityScore: 99, DeliveryRate: .99, MaxTPS: 100})
	_ = r.RegisterRule(Rule{ID: "primary", TenantID: "t", Prefix: "977", ProviderID: "primary", Priority: 1})
	_ = r.RegisterRule(Rule{ID: "secondary", TenantID: "t", Prefix: "977", ProviderID: "secondary", Priority: 2})
	d, err := r.Route(context.Background(), message.SubmitRequest{TenantID: "t", To: "9771"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Provider.ID != "primary" {
		t.Fatalf("priority must win before score, got %s", d.Provider.ID)
	}
}

func TestProviderStateAndSessionFilters(t *testing.T) {
	r := New()
	_ = r.RegisterProvider(provider.Generic34("down", "", "", ""))
	_ = r.RegisterProvider(provider.Generic34("ok", "", "", ""))
	_ = r.RegisterRule(Rule{ID: "down", TenantID: "t", Prefix: "977", ProviderID: "down", Priority: 1})
	_ = r.RegisterRule(Rule{ID: "ok", TenantID: "t", Prefix: "977", ProviderID: "ok", Priority: 1})
	r.SetProviderState("down", ProviderDown)
	r.UpdateSessionSnapshot(SessionSnapshot{ProviderID: "ok", Healthy: true, WindowSize: 10, WindowUsed: 2, State: "bound"})
	d, err := r.Route(context.Background(), message.SubmitRequest{TenantID: "t", To: "9771"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Provider.ID != "ok" {
		t.Fatalf("want ok got %s", d.Provider.ID)
	}
	if len(d.Rejected) == 0 {
		t.Fatal("expected rejected candidate explanation")
	}
}
