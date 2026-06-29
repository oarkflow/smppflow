package queue

import (
	"context"
	"errors"
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
