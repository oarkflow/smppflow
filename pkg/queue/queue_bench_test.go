package queue

// Benchmarks for the in-memory queue, covering the hot path every SMS job
// passes through: Enqueue (producer side) and Reserve+Ack (worker side).
//
// These are written against ONLY the exported Memory methods (Enqueue,
// Reserve, Ack, Nack, Close, ...) so they keep working across the concurrent
// heap-based rewrite of this package, regardless of internal representation.

import (
	"context"
	"testing"
	"time"
)

func benchJob(i int) Job {
	return Job{
		Type:     "sms",
		Payload:  i,
		Priority: i % 5,
	}
}

// BenchmarkMemoryQueueEnqueue measures pure producer throughput: how fast
// jobs can be pushed onto the queue, single goroutine.
func BenchmarkMemoryQueueEnqueue(b *testing.B) {
	q := NewMemory()
	defer q.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := q.Enqueue(ctx, benchJob(i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMemoryQueueEnqueueParallel measures producer throughput under
// concurrent load, as would happen with many SMPP sessions submitting at once.
func BenchmarkMemoryQueueEnqueueParallel(b *testing.B) {
	q := NewMemory()
	defer q.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := q.Enqueue(ctx, benchJob(i)); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkMemoryQueueEnqueueReserveAck measures the full round trip a job
// takes through the queue: producer enqueues, a worker reserves it, then acks
// it. Single goroutine (sequential enqueue/reserve/ack).
func BenchmarkMemoryQueueEnqueueReserveAck(b *testing.B) {
	q := NewMemory()
	defer q.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := q.Enqueue(ctx, benchJob(i)); err != nil {
			b.Fatal(err)
		}
		j, err := q.Reserve(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := q.Ack(ctx, j.ID); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMemoryQueueEnqueueReserveAckParallel drives concurrent producers
// and a pool of worker goroutines through Enqueue -> Reserve -> Ack, which is
// the realistic concurrent shape of an SMS gateway (many senders, many
// dispatch workers).
func BenchmarkMemoryQueueEnqueueReserveAckParallel(b *testing.B) {
	q := NewMemory()
	defer q.Close()
	ctx := context.Background()

	// Background workers continuously drain the queue so producers never
	// stall waiting for a Reserve() poll loop with nothing queued.
	const workers = 8
	stop := make(chan struct{})
	done := make(chan struct{}, workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for {
				select {
				case <-stop:
					return
				default:
				}
				wctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
				j, err := q.Reserve(wctx)
				cancel()
				if err != nil {
					continue
				}
				_ = q.Ack(ctx, j.ID)
			}
		}()
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := q.Enqueue(ctx, benchJob(i)); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
	b.StopTimer()
	close(stop)
	for w := 0; w < workers; w++ {
		<-done
	}
}

// BenchmarkMemoryQueueReserveEmpty measures the cost of a Reserve() call that
// finds nothing to do (common when workers outnumber producers briefly).
func BenchmarkMemoryQueueReserveEmpty(b *testing.B) {
	q := NewMemory()
	defer q.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, _ = q.Reserve(ctx)
		cancel()
	}
}
