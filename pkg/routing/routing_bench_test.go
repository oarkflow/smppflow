package routing

// Benchmarks for the route-selection hot path: Router.Route is called once
// per outbound SMS to pick the destination provider.

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/provider"
)

// newBenchRouter builds a router with a handful of providers and rules,
// roughly modeling a multi-provider, multi-tenant routing table.
func newBenchRouter(nProviders, nRulesPerProvider int) *Router {
	r := New()
	prefixes := []string{"1", "44", "91", "977", "61", "81", "49", "33"}
	for p := 0; p < nProviders; p++ {
		id := "provider-" + strconv.Itoa(p)
		prof := provider.Generic34(id, "addr"+strconv.Itoa(p), "sys"+strconv.Itoa(p), "pw")
		prof.TPS = 1000
		// Capacity reservations from the router's default in-memory capacity
		// manager are never released here (no Commit/Release calls), so a
		// realistically large window avoids RejectWindowFull once b.N grows
		// past the default WindowSize during long benchmark runs.
		prof.WindowSize = 1 << 30
		r.AddProvider(prof)
		r.UpdateProviderStats(ProviderStats{ProviderID: id, QualityScore: 70 + float64(p%30), DeliveryRate: 0.9, MaxTPS: 1000, CurrentTPS: p % 100})
		for k := 0; k < nRulesPerProvider; k++ {
			r.AddRule(Rule{
				ID:         id + "-rule-" + strconv.Itoa(k),
				Prefix:     prefixes[(p+k)%len(prefixes)],
				ProviderID: id,
				Priority:   1 + (k % 5),
				Weight:     1 + (p % 3),
			})
		}
	}
	return r
}

func BenchmarkRouterRoute(b *testing.B) {
	r := newBenchRouter(10, 5)
	req := message.SubmitRequest{To: "9779800000000", From: "SENDER"}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Route(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRouterRouteParallel(b *testing.B) {
	r := newBenchRouter(10, 5)
	req := message.SubmitRequest{To: "9779800000000", From: "SENDER"}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := r.Route(ctx, req); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRouterRouteMultiTenant exercises the tenant-scoped index path
// (byTenant / byTenantSender) rather than the global prefix trie.
func BenchmarkRouterRouteMultiTenant(b *testing.B) {
	r := New()
	for p := 0; p < 5; p++ {
		id := "provider-" + strconv.Itoa(p)
		prof := provider.Generic34(id, "addr"+strconv.Itoa(p), "sys"+strconv.Itoa(p), "pw")
		prof.TPS = 1000
		prof.WindowSize = 1 << 30
		r.AddProvider(prof)
		for t := 0; t < 20; t++ {
			tenant := "tenant-" + strconv.Itoa(t)
			r.AddRule(Rule{ID: id + "-" + tenant, TenantID: tenant, ProviderID: id, Priority: 1 + p})
		}
	}
	ctx := context.Background()
	req := message.SubmitRequest{To: "9779800000000", From: "SENDER", TenantID: "tenant-10"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Route(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRouterUpdateProviderStats(b *testing.B) {
	r := newBenchRouter(10, 5)
	st := ProviderStats{ProviderID: "provider-0", QualityScore: 90, DeliveryRate: 0.95, MaxTPS: 1000, CurrentTPS: 50, AvgLatency: 20 * time.Millisecond}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.UpdateProviderStats(st)
	}
}
