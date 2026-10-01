package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/storage"
)

func newMsg(id message.ID) *message.Message {
	return &message.Message{
		ID:       id,
		TenantID: "tenant-a",
		From:     protocol.Address{Addr: "BRAND"},
		To:       protocol.Address{Addr: "9779841000000"},
		Text:     "hello",
		State:    lifecycle.MsgCreated,
		Meta:     map[string]string{"country": "NP"},
		Parts:    []message.SegmentRecord{{ID: "seg-1", Index: 1, Total: 1}},
	}
}

func TestMemorySaveAndGet(t *testing.T) {
	ctx := context.Background()
	m := storage.NewMemory()
	if err := m.SaveMessage(ctx, newMsg("m1")); err != nil {
		t.Fatal(err)
	}
	got, err := m.GetMessage(ctx, "m1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "m1" || got.TenantID != "tenant-a" {
		t.Fatalf("got %+v", got)
	}
	if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Fatal("SaveMessage must stamp CreatedAt/UpdatedAt")
	}
}

func TestMemoryGetUnknownReturnsError(t *testing.T) {
	_, err := storage.NewMemory().GetMessage(context.Background(), "nope")
	if err == nil {
		t.Fatal("unknown id must return an error")
	}
}

func TestMemoryStoresDefensively(t *testing.T) {
	ctx := context.Background()
	m := storage.NewMemory()
	msg := newMsg("m1")
	if err := m.SaveMessage(ctx, msg); err != nil {
		t.Fatal(err)
	}
	// Mutating the caller's copy must not affect the stored one.
	msg.Text = "mutated"
	msg.Meta["country"] = "US"
	msg.Parts[0].ID = "seg-x"
	got, err := m.GetMessage(ctx, "m1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Text != "hello" {
		t.Fatalf("stored text mutated: %q", got.Text)
	}
	if got.Meta["country"] != "NP" {
		t.Fatalf("stored meta mutated: %v", got.Meta)
	}
	if got.Parts[0].ID != "seg-1" {
		t.Fatalf("stored parts mutated: %v", got.Parts)
	}
	// And the returned copy must be safe to mutate too.
	got.Text = "changed"
	again, _ := m.GetMessage(ctx, "m1")
	if again.Text != "hello" {
		t.Fatalf("read leaked mutation: %q", again.Text)
	}
}

func TestMemoryListMessages(t *testing.T) {
	ctx := context.Background()
	m := storage.NewMemory()
	for _, id := range []message.ID{"c", "a", "b"} {
		if err := m.SaveMessage(ctx, newMsg(id)); err != nil {
			t.Fatal(err)
		}
	}
	xs, err := m.ListMessages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(xs) != 3 {
		t.Fatalf("len=%d want 3", len(xs))
	}
	// Newest first; IDs tiebreak so equal CreatedAt values stay deterministic.
	for i := 1; i < len(xs); i++ {
		if xs[i-1].CreatedAt.Before(xs[i].CreatedAt) {
			t.Fatalf("list is not newest-first at index %d", i)
		}
	}
	if xs[0].ID == xs[1].ID || xs[1].ID == xs[2].ID {
		t.Fatal("expected three distinct ids")
	}
}

func TestMemoryUpdateMessageState(t *testing.T) {
	ctx := context.Background()
	m := storage.NewMemory()
	if err := m.SaveMessage(ctx, newMsg("m1")); err != nil {
		t.Fatal(err)
	}
	if err := m.UpdateMessageState(ctx, "m1", lifecycle.MsgSubmitted); err != nil {
		t.Fatal(err)
	}
	got, _ := m.GetMessage(ctx, "m1")
	if got.State != lifecycle.MsgSubmitted {
		t.Fatalf("state=%v want submitted", got.State)
	}
	if err := m.UpdateMessageState(ctx, "missing", lifecycle.MsgFailed); err == nil {
		t.Fatal("updating a missing message must error")
	}
}

func TestMemoryDLRAndCorrelation(t *testing.T) {
	ctx := context.Background()
	m := storage.NewMemory()
	msg := newMsg("m1")
	msg.ProviderID = "prov-1"
	msg.ProviderMessageID = "pid-1"
	msg.Parts[0].ProviderMessageID = "pid-1"
	if err := m.SaveMessage(ctx, msg); err != nil {
		t.Fatal(err)
	}
	// A DLR cannot jump a message from created straight to delivered; walk a
	// legal path first.
	if err := m.UpdateMessageState(ctx, "m1", lifecycle.MsgSubmitted); err != nil {
		t.Fatal(err)
	}
	dlr := &message.DeliveryReceipt{
		MessageID:         "m1",
		ProviderMessageID: "pid-1",
		ProviderID:        "prov-1",
		State:             lifecycle.MsgDelivered,
		DoneAt:            time.Now(),
	}
	if err := m.SaveDLR(ctx, dlr); err != nil {
		t.Fatal(err)
	}
	xs, err := m.ListDLR(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(xs) != 1 {
		t.Fatalf("dlrs=%d want 1", len(xs))
	}
	got, _ := m.GetMessage(ctx, "m1")
	if got.State != lifecycle.MsgDelivered {
		t.Fatalf("message state=%v want delivered after DLR", got.State)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("DLR must refresh UpdatedAt")
	}
}

func TestMemoryDLRSegmentLevel(t *testing.T) {
	ctx := context.Background()
	m := storage.NewMemory()
	msg := newMsg("m1")
	msg.Parts = []message.SegmentRecord{
		{ID: "seg-1", Index: 1, Total: 2},
		{ID: "seg-2", Index: 2, Total: 2},
	}
	if err := m.SaveMessage(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if err := m.SaveDLR(ctx, &message.DeliveryReceipt{SegmentID: "seg-1", State: lifecycle.MsgDelivered}); err != nil {
		t.Fatal(err)
	}
	got, _ := m.GetMessage(ctx, "m1")
	if got.Parts[0].State != lifecycle.MsgDelivered {
		t.Fatalf("seg-1 state=%v want delivered", got.Parts[0].State)
	}
	if got.Parts[1].State == lifecycle.MsgDelivered {
		t.Fatal("seg-2 must be untouched by seg-1's DLR")
	}
	if got.State != lifecycle.MsgPartiallyDelivered {
		t.Fatalf("aggregate state=%v want partially_delivered", got.State)
	}
}

func TestMemoryDLRIgnoresIllegalTransition(t *testing.T) {
	ctx := context.Background()
	m := storage.NewMemory()
	if err := m.SaveMessage(ctx, newMsg("m1")); err != nil {
		t.Fatal(err)
	}
	// created -> delivered is not a legal lifecycle transition, so the DLR must
	// be recorded without corrupting the message state.
	if err := m.SaveDLR(ctx, &message.DeliveryReceipt{MessageID: "m1", State: lifecycle.MsgDelivered}); err != nil {
		t.Fatal(err)
	}
	got, _ := m.GetMessage(ctx, "m1")
	if got.State != lifecycle.MsgCreated {
		t.Fatalf("state=%v want created (illegal DLR transition must not apply)", got.State)
	}
	xs, _ := m.ListDLR(ctx)
	if len(xs) != 1 {
		t.Fatalf("the DLR itself must still be retained, got %d", len(xs))
	}
}

func TestMemoryDLRUnmatchedIsRetained(t *testing.T) {
	ctx := context.Background()
	m := storage.NewMemory()
	if err := m.SaveDLR(ctx, &message.DeliveryReceipt{MessageID: "unknown", State: lifecycle.MsgDelivered}); err != nil {
		t.Fatal(err)
	}
	xs, _ := m.ListDLR(ctx)
	if len(xs) != 1 {
		t.Fatalf("unmatched DLR must still be retained, got %d", len(xs))
	}
}

func TestMemoryIdempotency(t *testing.T) {
	ctx := context.Background()
	m := storage.NewMemory()
	rec := message.IdempotencyRecord{
		TenantID:    "tenant-a",
		Key:         "key-1",
		PayloadHash: "hash",
		MessageID:   "m1",
		Result:      message.SubmitResult{MessageID: "m1", Parts: 1},
		CreatedAt:   time.Now(),
		ExpiresAt:   time.Now().Add(time.Hour),
	}
	if err := m.SaveIdempotency(ctx, rec); err != nil {
		t.Fatal(err)
	}
	got, err := m.GetIdempotency(ctx, "tenant-a", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.MessageID != "m1" || got.PayloadHash != "hash" {
		t.Fatalf("got %+v", got)
	}
	if _, err := m.GetIdempotency(ctx, "tenant-a", "missing"); err == nil {
		t.Fatal("missing idempotency key must error")
	}
	// A different tenant with the same key is a different record.
	if _, err := m.GetIdempotency(ctx, "tenant-b", "key-1"); err == nil {
		t.Fatal("idempotency must be tenant-scoped")
	}
}

func TestMemoryTransitions(t *testing.T) {
	ctx := context.Background()
	m := storage.NewMemory()
	tr := message.Transition{
		MessageID: "m1",
		From:      lifecycle.MsgCreated,
		To:        lifecycle.MsgQueued,
		Actor:     "worker-1",
		At:        time.Now(),
	}
	if err := m.SaveTransition(ctx, tr); err != nil {
		t.Fatal(err)
	}
	xs, err := m.ListTransitions(ctx, message.EventFilter{MessageID: "m1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(xs) != 1 {
		t.Fatalf("transitions=%d want 1", len(xs))
	}
	if xs[0].To != lifecycle.MsgQueued {
		t.Fatalf("to=%v", xs[0].To)
	}
	// Filter by a different message id yields nothing.
	if xs, _ := m.ListTransitions(ctx, message.EventFilter{MessageID: "other"}); len(xs) != 0 {
		t.Fatalf("filter mismatch: %v", xs)
	}
}

func TestMemoryIdempotencyOverwrite(t *testing.T) {
	ctx := context.Background()
	m := storage.NewMemory()
	base := message.IdempotencyRecord{TenantID: "t", Key: "k", MessageID: "m1", CreatedAt: time.Now()}
	if err := m.SaveIdempotency(ctx, base); err != nil {
		t.Fatal(err)
	}
	base.MessageID = "m2"
	if err := m.SaveIdempotency(ctx, base); err != nil {
		t.Fatal(err)
	}
	got, _ := m.GetIdempotency(ctx, "t", "k")
	if got.MessageID != "m2" {
		t.Fatalf("message id=%v want m2", got.MessageID)
	}
}

func TestMemoryConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	m := storage.NewMemory()
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 50; j++ {
				id := message.ID("m")
				_ = m.SaveMessage(ctx, newMsg(id))
				_, _ = m.GetMessage(ctx, id)
				_, _ = m.ListMessages(ctx)
				_ = m.UpdateMessageState(ctx, id, lifecycle.MsgSending)
				_ = m.SaveDLR(ctx, &message.DeliveryReceipt{MessageID: string(id), State: lifecycle.MsgDelivered})
				_, _ = m.ListDLR(ctx)
			}
		}(i)
	}
	for i := 0; i < 8; i++ {
		<-done
	}
}
