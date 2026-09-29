package durablequeue

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/queue"
)

func TestBrokerQueue_EnqueueReserveAck(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	bq, err := NewBrokerQueue(ctx, filepath.Join(dir, "wal"), "jobs.test", WithVisibilityTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("NewBrokerQueue: %v", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = bq.Close(stopCtx)
	}()

	job := queue.Job{ID: "job-1", Type: "smpp.send", Payload: map[string]any{"to": "123"}, MaxAttempts: 3}
	if err := bq.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := bq.Reserve(ctx)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if got == nil {
		t.Fatal("Reserve returned nil job")
	}
	if got.Meta["_job_id"] != "job-1" {
		t.Fatalf("expected original job id preserved in meta, got %q", got.Meta["_job_id"])
	}
	if got.Type != "smpp.send" {
		t.Fatalf("expected type smpp.send, got %q", got.Type)
	}
	if got.State != queue.JobReserved {
		t.Fatalf("expected state reserved, got %q", got.State)
	}

	// Nothing else to reserve right now.
	if _, err := bq.Reserve(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("expected ErrNoJob on empty queue, got %v", err)
	}

	if err := bq.Ack(ctx, got.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Acking twice should fail (lease already consumed).
	if err := bq.Ack(ctx, got.ID); err == nil {
		t.Fatal("expected error acking an already-acked message")
	}
}

func TestBrokerQueue_NackRequeue(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	bq, err := NewBrokerQueue(ctx, filepath.Join(dir, "wal"), "jobs.nack", WithVisibilityTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("NewBrokerQueue: %v", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = bq.Close(stopCtx)
	}()

	job := queue.Job{ID: "job-2", Type: "smpp.send", Payload: "hello", MaxAttempts: 3}
	if err := bq.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := bq.Reserve(ctx)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	got.Attempts = 1 // simulate first delivery attempt

	if err := bq.Nack(ctx, got, errors.New("temporary failure"), 0); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// Should be requeued and immediately reservable again (delay=0).
	deadline := time.Now().Add(2 * time.Second)
	var retried *queue.Job
	for time.Now().Before(deadline) {
		retried, err = bq.Reserve(ctx)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrNoJob) {
			t.Fatalf("Reserve after nack: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if retried == nil {
		t.Fatal("expected requeued job to be reservable again")
	}
	if retried.Meta["_job_id"] != "job-2" {
		t.Fatalf("expected original job id preserved after nack/retry, got %q", retried.Meta["_job_id"])
	}
	if err := bq.Ack(ctx, retried.ID); err != nil {
		t.Fatalf("Ack after retry: %v", err)
	}
}

func TestBrokerQueue_NackExhaustedGoesToDLQ(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	bq, err := NewBrokerQueue(ctx, filepath.Join(dir, "wal"), "jobs.dlq", WithVisibilityTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("NewBrokerQueue: %v", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = bq.Close(stopCtx)
	}()

	job := queue.Job{ID: "job-3", Type: "smpp.send", MaxAttempts: 1}
	if err := bq.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := bq.Reserve(ctx)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	got.Attempts = got.MaxAttempts // exhausted

	if err := bq.Nack(ctx, got, errors.New("permanent failure"), 0); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// The main queue should now be empty (message moved to DLQ, not requeued).
	time.Sleep(50 * time.Millisecond)
	if _, err := bq.Reserve(ctx); !errors.Is(err, ErrNoJob) {
		t.Fatalf("expected ErrNoJob after DLQ move, got %v", err)
	}
}

// TestBrokerQueue_WALSurvivesRestart is the actual value proposition of this
// backend: an unacked job persisted to the WAL must still be there (and
// still reservable) after the broker process (and *broker.Broker instance)
// is stopped and a brand new one is opened against the same data directory,
// simulating a process restart/crash recovery.
func TestBrokerQueue_WALSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "wal")
	const queueName = "jobs.durable"

	first, err := NewBrokerQueue(ctx, dir, queueName, WithVisibilityTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("NewBrokerQueue (first): %v", err)
	}

	job := queue.Job{ID: "job-durable-1", Type: "smpp.send", Payload: map[string]any{"msisdn": "999"}, MaxAttempts: 5}
	if err := first.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Also reserve-but-don't-ack a second job, so we exercise recovery of an
	// in-flight lease, not just a queued message. FIFO ordering means this
	// Reserve pulls job-durable-1 (leaving job-durable-2 untouched in the
	// queue); track both application-level job IDs by set, not by name, so
	// the test doesn't depend on which of the two ends up "in-flight" vs
	// "still queued" at crash time.
	job2 := queue.Job{ID: "job-durable-2", Type: "smpp.send", MaxAttempts: 5}
	if err := first.Enqueue(ctx, job2); err != nil {
		t.Fatalf("Enqueue job2: %v", err)
	}
	reserved2, err := first.Reserve(ctx)
	if err != nil {
		t.Fatalf("Reserve job2: %v", err)
	}
	inFlightJobID := reserved2.Meta["_job_id"]
	if inFlightJobID != "job-durable-1" && inFlightJobID != "job-durable-2" {
		t.Fatalf("unexpected reserved job: %+v", reserved2)
	}
	// Deliberately do not Ack reserved2, to simulate a crash mid-processing.

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := first.Close(stopCtx); err != nil {
		cancel()
		t.Fatalf("Close first broker: %v", err)
	}
	cancel()

	// Reopen against the same data directory: this is the restart.
	second, err := NewBrokerQueue(ctx, dir, queueName, WithVisibilityTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("NewBrokerQueue (second, after restart): %v", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = second.Close(stopCtx)
	}()

	// The queued-but-untouched job must still be queued and immediately
	// reservable. The in-flight (leased-but-unacked) job must also still be
	// present: broker's WAL recovery resets any message that was
	// leased/delivered at crash time back to queued (see
	// normalizeRecoveredInflight), so it should be immediately reservable
	// too — not lost, and not stuck waiting out its old visibility timeout
	// forever. Poll briefly to allow for that, then assert both of the
	// original job IDs come back exactly once.
	seen := map[string]bool{}
	deadline := time.Now().Add(10 * time.Second)
	for len(seen) < 2 && time.Now().Before(deadline) {
		got, err := second.Reserve(ctx)
		if err != nil {
			if errors.Is(err, ErrNoJob) {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			t.Fatalf("Reserve after restart: %v", err)
		}
		id := got.Meta["_job_id"]
		if seen[id] {
			t.Fatalf("job %q redelivered twice within this loop", id)
		}
		seen[id] = true
		if err := second.Ack(ctx, got.ID); err != nil {
			t.Fatalf("Ack %q after restart: %v", id, err)
		}
	}
	if !seen["job-durable-1"] || !seen["job-durable-2"] {
		t.Fatalf("expected both job-durable-1 and job-durable-2 to survive the restart, got %v", seen)
	}
}
