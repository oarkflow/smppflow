package ratelimit

// Benchmarks for the token-bucket rate limiter's Allow() check, which runs on
// every outbound SMS submission (per tenant/provider/route key).

import (
	"context"
	"strconv"
	"testing"
)

func newBenchLimiter(nKeys int) (*Limiter, []string) {
	l := New()
	keys := make([]string, nKeys)
	for i := 0; i < nKeys; i++ {
		k := "tenant:" + strconv.Itoa(i)
		keys[i] = k
		if err := l.SetPolicy(Policy{Key: k, RatePerSecond: 1_000_000, Burst: 1_000_000}); err != nil {
			panic(err)
		}
	}
	return l, keys
}

// BenchmarkLimiterAllowSingleKey measures the common case: one key, checked
// repeatedly (e.g. a single busy tenant or provider route).
func BenchmarkLimiterAllowSingleKey(b *testing.B) {
	l, keys := newBenchLimiter(1)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.Allow(ctx, keys[0], 1); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLimiterAllowSingleKeyParallel measures contention on a single
// bucket's mutex under concurrent senders, which is the realistic shape for
// a shared tenant or provider limit.
func BenchmarkLimiterAllowSingleKeyParallel(b *testing.B) {
	l, keys := newBenchLimiter(1)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := l.Allow(ctx, keys[0], 1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkLimiterAllowManyKeys measures lookups across many distinct buckets
// (e.g. many tenants/routes sharing one limiter instance).
func BenchmarkLimiterAllowManyKeys(b *testing.B) {
	const n = 1000
	l, keys := newBenchLimiter(n)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.Allow(ctx, keys[i%n], 1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLimiterAllowManyKeysParallel(b *testing.B) {
	const n = 1000
	l, keys := newBenchLimiter(n)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := l.Allow(ctx, keys[i%n], 1); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
