package queue

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestQueueRetryDLQ(t *testing.T) {
	q := NewMemory()
	ctx := context.Background()
	_ = q.Enqueue(ctx, Job{ID: "j1", Type: "x", MaxAttempts: 1, RunAt: time.Now()})
	j, err := q.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = q.Nack(ctx, j, errors.New("boom"), 0)
	if len(q.DLQ()) != 1 {
		t.Fatalf("dlq=%d", len(q.DLQ()))
	}
}

// TestReservePriorityOrder ensures the heap-based selection still picks the
// highest-priority ready job first, and falls back to FIFO (CreatedAt) for
// equal priorities — the same rule the old O(n) scan implemented.
func TestReservePriorityOrder(t *testing.T) {
	q := NewMemory()
	ctx := context.Background()
	now := time.Now()
	_ = q.Enqueue(ctx, Job{ID: "low-first", Priority: 0, RunAt: now, CreatedAt: now})
	_ = q.Enqueue(ctx, Job{ID: "low-second", Priority: 0, RunAt: now, CreatedAt: now.Add(time.Millisecond)})
	_ = q.Enqueue(ctx, Job{ID: "high", Priority: 5, RunAt: now, CreatedAt: now.Add(2 * time.Millisecond)})

	order := []string{}
	for i := 0; i < 3; i++ {
		j, err := q.Reserve(ctx)
		if err != nil {
			t.Fatal(err)
		}
		order = append(order, j.ID)
	}
	want := []string{"high", "low-first", "low-second"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("reserve order = %v, want %v", order, want)
		}
	}
}

// TestReserveVisibilityDelay ensures a job with a future RunAt is not
// reservable until it becomes due, and is picked up promptly afterward.
func TestReserveVisibilityDelay(t *testing.T) {
	q := NewMemory()
	ctx := context.Background()
	delay := 150 * time.Millisecond
	_ = q.Enqueue(ctx, Job{ID: "delayed", RunAt: time.Now().Add(delay)})

	shortCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, err := q.Reserve(shortCtx); err == nil {
		t.Fatal("expected reserve to time out before job becomes due")
	}

	longCtx, cancel2 := context.WithTimeout(ctx, time.Second)
	defer cancel2()
	j, err := q.Reserve(longCtx)
	if err != nil {
		t.Fatalf("reserve after due: %v", err)
	}
	if j.ID != "delayed" {
		t.Fatalf("got job %s, want delayed", j.ID)
	}
}

// TestNackRequeueAndRetryOrdering verifies Nack requeues below MaxAttempts
// and dead-letters once MaxAttempts is reached, preserving prior semantics.
func TestNackRequeueAndRetryOrdering(t *testing.T) {
	q := NewMemory()
	ctx := context.Background()
	_ = q.Enqueue(ctx, Job{ID: "retry", MaxAttempts: 2, RunAt: time.Now()})
	j, err := q.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Nack(ctx, j, errors.New("transient"), 0); err != nil {
		t.Fatal(err)
	}
	if len(q.DLQ()) != 0 {
		t.Fatalf("expected no DLQ entries yet, got %d", len(q.DLQ()))
	}
	j2, err := q.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if j2.ID != "retry" {
		t.Fatalf("got %s, want retry", j2.ID)
	}
	if err := q.Nack(ctx, j2, errors.New("fatal"), 0); err != nil {
		t.Fatal(err)
	}
	if len(q.DLQ()) != 1 {
		t.Fatalf("expected job dead-lettered after MaxAttempts, dlq=%d", len(q.DLQ()))
	}
}

// TestConcurrentReserveNoDuplicateOrLoss exercises concurrent Enqueue/Reserve
// to confirm the heap-based queue delivers every job exactly once under
// concurrent access.
func TestConcurrentReserveNoDuplicateOrLoss(t *testing.T) {
	q := NewMemory()
	ctx := context.Background()
	const n = 496 // evenly divisible by the worker count below
	for i := 0; i < n; i++ {
		id := "job-" + strconv.Itoa(i)
		_ = q.Enqueue(ctx, Job{ID: id, RunAt: time.Now()})
	}

	var mu sync.Mutex
	seen := make(map[string]int)
	var wg sync.WaitGroup
	workers := 8
	perWorker := n / workers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				j, err := q.Reserve(rctx)
				cancel()
				if err != nil {
					t.Errorf("reserve: %v", err)
					return
				}
				mu.Lock()
				seen[j.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("expected %d distinct jobs reserved, got %d", n, len(seen))
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("job %s reserved %d times", id, count)
		}
	}
}
