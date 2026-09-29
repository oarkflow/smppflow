// Package loadtest is a small, stdlib-only load harness for pkg/queue's
// in-memory queue. It pushes N messages concurrently through the full
// Enqueue -> Reserve -> Ack cycle (the same path an SMS gateway uses to hand
// a submitted message to a dispatch worker) and reports throughput and
// latency percentiles.
//
// It is intentionally a `go test` (not a Benchmark) so it produces one clear,
// reportable number per run instead of go test -bench's iteration tuning.
// Run it explicitly with:
//
//	go test ./pkg/queue/loadtest/... -run TestQueueLoad -v
//
// N can be overridden with -load.n, producers/consumers with -load.producers
// / -load.consumers.
package loadtest

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/queue"
)

var (
	flagN         = flag.Int("load.n", 100_000, "number of messages to push through the queue")
	flagProducers = flag.Int("load.producers", 32, "number of concurrent producer goroutines")
	flagConsumers = flag.Int("load.consumers", 32, "number of concurrent consumer (worker) goroutines")
)

// percentile returns the p-th percentile (0..100) of a sorted duration slice
// using nearest-rank, which is sufficient for a reportable ballpark figure.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p/100*float64(len(sorted)-1) + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func TestQueueLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in -short mode")
	}

	n := *flagN
	producers := *flagProducers
	consumers := *flagConsumers

	q := queue.NewMemory()
	defer q.Close()

	latencies := make([]time.Duration, n)
	var produced int64
	var consumed int64

	overallStart := time.Now()

	// Producers: enqueue n jobs total, split across `producers` goroutines.
	var produceWG sync.WaitGroup
	perProducer := n / producers
	remainder := n % producers
	for p := 0; p < producers; p++ {
		count := perProducer
		if p < remainder {
			count++
		}
		produceWG.Add(1)
		go func(count int) {
			defer produceWG.Done()
			ctx := context.Background()
			for i := 0; i < count; i++ {
				j := queue.Job{Type: "sms", Payload: time.Now()}
				if err := q.Enqueue(ctx, j); err != nil {
					t.Errorf("enqueue failed: %v", err)
					return
				}
				atomic.AddInt64(&produced, 1)
			}
		}(count)
	}

	// Consumers: reserve + ack until all n jobs have been consumed.
	var consumeWG sync.WaitGroup
	for c := 0; c < consumers; c++ {
		consumeWG.Add(1)
		go func(idx int) {
			defer consumeWG.Done()
			ctx := context.Background()
			for {
				total := atomic.LoadInt64(&consumed)
				if total >= int64(n) {
					return
				}
				rctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
				job, err := q.Reserve(rctx)
				cancel()
				if err != nil {
					// Either closed, cancelled, or nothing to reserve yet.
					if atomic.LoadInt64(&consumed) >= int64(n) {
						return
					}
					continue
				}
				enqueuedAt, _ := job.Payload.(time.Time)
				if err := q.Ack(ctx, job.ID); err != nil {
					t.Errorf("ack failed: %v", err)
					return
				}
				seq := atomic.AddInt64(&consumed, 1)
				if !enqueuedAt.IsZero() && int(seq)-1 < len(latencies) {
					latencies[seq-1] = time.Since(enqueuedAt)
				}
			}
		}(c)
	}

	produceWG.Wait()
	consumeWG.Wait()
	elapsed := time.Since(overallStart)

	if got := atomic.LoadInt64(&produced); got != int64(n) {
		t.Fatalf("produced %d, want %d", got, n)
	}
	if got := atomic.LoadInt64(&consumed); got != int64(n) {
		t.Fatalf("consumed %d, want %d", got, n)
	}

	// latencies may contain a few zero entries if the slot bookkeeping raced
	// (harmless for a ballpark percentile report); filter them out.
	nonZero := latencies[:0]
	for _, d := range latencies {
		if d > 0 {
			nonZero = append(nonZero, d)
		}
	}
	sort.Slice(nonZero, func(i, j int) bool { return nonZero[i] < nonZero[j] })

	throughput := float64(n) / elapsed.Seconds()
	p50 := percentile(nonZero, 50)
	p95 := percentile(nonZero, 95)
	p99 := percentile(nonZero, 99)

	msg := fmt.Sprintf(
		"queue load test: n=%d producers=%d consumers=%d elapsed=%s throughput=%.0f msgs/sec p50=%s p95=%s p99=%s",
		n, producers, consumers, elapsed, throughput, p50, p95, p99,
	)
	t.Log(msg)
	fmt.Println(msg)
}
