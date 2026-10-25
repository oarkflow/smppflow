// Package loadtest is a stdlib-only load harness for pkg/durablequeue's
// Backend implementations. It pushes N jobs through the Enqueue -> Reserve ->
// Ack cycle (and a pure-Enqueue burst) against the broker WAL queue, the Redis
// Streams queue and pkg/queue's in-memory queue as a baseline, and reports
// throughput plus latency percentiles in the same style as pkg/queue/loadtest.
//
// Run it explicitly with:
//
//	SMPPFLOW_LOADTEST=1 go test ./pkg/durablequeue/loadtest/... -run TestDurableQueueLoad -v
//
// The harness is opt-in via SMPPFLOW_LOADTEST because the broker WAL backend
// processes only a few hundred jobs per second and a full default run takes
// minutes. The Redis backend needs a reachable server: set
// SMPPFLOW_TEST_REDIS_ADDR (and optionally SMPPFLOW_TEST_REDIS_PASSWORD),
// otherwise it is skipped. N can be overridden with -load.n, producers/
// consumers with -load.producers / -load.consumers, backend selection with
// -load.backend (all|memory|broker|redis).
package loadtest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/durablequeue"
	"github.com/oarkflow/smppflow/pkg/queue"
)

var (
	flagN         = flag.Int("load.n", 20_000, "number of jobs to push through the queue")
	flagProducers = flag.Int("load.producers", 8, "number of concurrent producer goroutines")
	flagConsumers = flag.Int("load.consumers", 8, "number of concurrent consumer goroutines")
	flagBackend   = flag.String("load.backend", "all", "backend to measure: all|memory|broker|redis")
	flagBrokerBin = flag.Bool("load.broker-binary", false, "use the broker's binary WAL store instead of the JSONL one")
)

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

type harness struct {
	name string
	q    queue.Backend
}

type factory struct {
	name string
	make func(t *testing.T) harness
}

func brokerFactory() factory {
	name := "broker(wal)"
	if *flagBrokerBin {
		name = "broker(binary-wal)"
	}
	return factory{name: name, make: func(t *testing.T) harness {
		t.Helper()
		dir := t.TempDir()
		opts := []durablequeue.BrokerQueueOption{durablequeue.WithVisibilityTimeout(2 * time.Second)}
		if *flagBrokerBin {
			opts = append(opts, durablequeue.WithBinaryWAL())
		}
		bq, err := durablequeue.NewBrokerQueue(context.Background(), filepath.Join(dir, "wal"), "jobs.loadtest", opts...)
		if err != nil {
			t.Fatalf("NewBrokerQueue: %v", err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = bq.Close(ctx)
		})
		return harness{name: name, q: bq}
	}}
}

func redisFactory(t *testing.T) (factory, bool) {
	t.Helper()
	addr := os.Getenv("SMPPFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		return factory{}, false
	}
	d := net.Dialer{Timeout: time.Second}
	conn, err := d.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		t.Logf("redis unreachable at %s, skipping: %v", addr, err)
		return factory{}, false
	}
	_ = conn.Close()
	return factory{name: "redis(streams)", make: func(t *testing.T) harness {
		t.Helper()
		rq := durablequeue.RedisStreamsQueue{
			Addr:     addr,
			Password: os.Getenv("SMPPFLOW_TEST_REDIS_PASSWORD"),
			Stream:   fmt.Sprintf("smppflow:loadtest:%d", time.Now().UnixNano()),
			Group:    "loadtest",
			Consumer: "loadtest-1",
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rq.EnsureGroup(ctx); err != nil {
			t.Fatalf("EnsureGroup: %v", err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = rq.Shutdown(ctx)
		})
		return harness{name: "redis(streams)", q: rq}
	}}, true
}

func memoryFactory() factory {
	return factory{name: "memory", make: func(t *testing.T) harness {
		t.Helper()
		mem := queue.NewMemory()
		t.Cleanup(mem.Close)
		return harness{name: "memory", q: mem}
	}}
}

func runBurst(t *testing.T, h harness, n, producers int) {
	t.Helper()
	var wg sync.WaitGroup
	per := n / producers
	start := time.Now()
	for p := 0; p < producers; p++ {
		count := per
		if p < n%producers {
			count++
		}
		wg.Add(1)
		go func(count int) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < count; i++ {
				j := queue.Job{ID: fmt.Sprintf("burst-%d-%d", time.Now().UnixNano(), i), Type: "smpp.send", MaxAttempts: 3,
					Meta: map[string]string{"enq_ns": strconv.FormatInt(time.Now().UnixNano(), 10)}}
				if err := h.q.Enqueue(ctx, j); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(count)
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%s burst enqueue: n=%d producers=%d elapsed=%v throughput=%.0f enqueues/sec\n",
		h.name, n, producers, elapsed, float64(n)/elapsed.Seconds())
}

func runRoundTrip(t *testing.T, h harness, n, producers, consumers int) {
	t.Helper()
	latencies := make([]time.Duration, n)
	var consumed int64

	start := time.Now()
	var produceWG sync.WaitGroup
	per := n / producers
	for p := 0; p < producers; p++ {
		count := per
		if p < n%producers {
			count++
		}
		produceWG.Add(1)
		go func(count int) {
			defer produceWG.Done()
			ctx := context.Background()
			for i := 0; i < count; i++ {
				j := queue.Job{Type: "smpp.send", MaxAttempts: 3,
					Meta: map[string]string{"enq_ns": strconv.FormatInt(time.Now().UnixNano(), 10)}}
				if err := h.q.Enqueue(ctx, j); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(count)
	}

	var consumeWG sync.WaitGroup
	for c := 0; c < consumers; c++ {
		consumeWG.Add(1)
		go func() {
			defer consumeWG.Done()
			ctx := context.Background()
			for {
				if atomic.LoadInt64(&consumed) >= int64(n) {
					return
				}
				rctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				job, err := h.q.Reserve(rctx)
				cancel()
				if err != nil {
					if errors.Is(err, queue.ErrNoJob) || errors.Is(err, durablequeue.ErrNoJob) || rctx.Err() != nil {
						if atomic.LoadInt64(&consumed) >= int64(n) {
							return
						}
						continue
					}
					if atomic.LoadInt64(&consumed) >= int64(n) {
						return
					}
					t.Errorf("reserve: %v", err)
					return
				}
				if err := h.q.Ack(ctx, job.ID); err != nil {
					t.Errorf("ack: %v", err)
					return
				}
				seq := atomic.AddInt64(&consumed, 1)
				if ns, err := strconv.ParseInt(job.Meta["enq_ns"], 10, 64); err == nil && int(seq)-1 < len(latencies) {
					latencies[int(seq)-1] = time.Since(time.Unix(0, ns))
				}
			}
		}()
	}

	produceWG.Wait()
	consumeWG.Wait()
	elapsed := time.Since(start)

	if got := atomic.LoadInt64(&consumed); got != int64(n) {
		t.Fatalf("consumed %d, want %d", got, n)
	}
	valid := latencies[:0]
	for _, l := range latencies {
		if l > 0 {
			valid = append(valid, l)
		}
	}
	sort.Slice(valid, func(i, j int) bool { return valid[i] < valid[j] })
	fmt.Printf("%s round trip: n=%d producers=%d consumers=%d elapsed=%v throughput=%.0f jobs/sec p50=%v p95=%v p99=%v\n",
		h.name, n, producers, consumers, elapsed, float64(n)/elapsed.Seconds(),
		percentile(valid, 50), percentile(valid, 95), percentile(valid, 99))
}

func TestDurableQueueLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in -short mode")
	}
	if os.Getenv("SMPPFLOW_LOADTEST") == "" {
		t.Skip("set SMPPFLOW_LOADTEST=1 to run the durablequeue load harness")
	}
	n := *flagN
	producers := *flagProducers
	consumers := *flagConsumers

	var factories []factory
	switch *flagBackend {
	case "memory":
		factories = append(factories, memoryFactory())
	case "broker":
		factories = append(factories, brokerFactory())
	case "redis":
		f, ok := redisFactory(t)
		if !ok {
			t.Skip("SMPPFLOW_TEST_REDIS_ADDR not set or redis unreachable")
		}
		factories = append(factories, f)
	default:
		factories = append(factories, memoryFactory(), brokerFactory())
		if f, ok := redisFactory(t); ok {
			factories = append(factories, f)
		} else {
			t.Log("redis backend skipped (SMPPFLOW_TEST_REDIS_ADDR not set or unreachable)")
		}
	}

	for _, f := range factories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			runBurst(t, f.make(t), n, producers)
			runRoundTrip(t, f.make(t), n, producers, consumers)
		})
	}
}
