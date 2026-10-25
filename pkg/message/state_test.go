package message_test

import (
	"testing"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
)

func TestCanTransition(t *testing.T) {
	if !message.CanTransition("", lifecycle.MsgCreated) {
		t.Fatal("empty -> created must be allowed")
	}
	if !message.CanTransition(lifecycle.MsgSegmented, lifecycle.MsgSubmitted) {
		t.Fatal("segmented -> submitted must be allowed")
	}
	if !message.CanTransition(lifecycle.MsgDelivered, lifecycle.MsgDelivered) {
		t.Fatal("same-state transition must be allowed")
	}
	if message.CanTransition(lifecycle.MsgDelivered, lifecycle.MsgSending) {
		t.Fatal("delivered -> sending must be rejected")
	}
	if message.CanTransition(lifecycle.MsgCreated, lifecycle.MsgDelivered) {
		t.Fatal("created -> delivered must be rejected")
	}
	if message.CanTransition(lifecycle.MsgArchived, lifecycle.MsgQueued) {
		t.Fatal("archived is terminal")
	}
}

func TestApplyTransition(t *testing.T) {
	msg := &message.Message{ID: "m1", State: lifecycle.MsgCreated}
	err := message.ApplyTransition(msg, lifecycle.MsgSegmented, "segmented", "manager", "prov", "")
	if err != nil {
		t.Fatalf("ApplyTransition: %v", err)
	}
	if msg.State != lifecycle.MsgSegmented {
		t.Fatalf("State=%q", msg.State)
	}
	if msg.UpdatedAt.IsZero() {
		t.Fatal("UpdatedAt must be set")
	}
	if len(msg.Transitions) != 1 {
		t.Fatalf("transitions=%d want 1", len(msg.Transitions))
	}
	tr := msg.Transitions[0]
	if tr.From != lifecycle.MsgCreated || tr.To != lifecycle.MsgSegmented || tr.Reason != "segmented" || tr.Actor != "manager" || tr.ProviderID != "prov" {
		t.Fatalf("transition=%+v", tr)
	}
	if tr.MessageID != "m1" || tr.At.IsZero() {
		t.Fatalf("transition metadata=%+v", tr)
	}

	if err := message.ApplyTransition(msg, lifecycle.MsgSubmitted, "", "", "", ""); err != nil {
		t.Fatalf("segmented -> submitted is allowed: %v", err)
	}
	if err := message.ApplyTransition(msg, lifecycle.MsgDelivered, "", "", "", ""); err != nil {
		t.Fatalf("submitted -> delivered is allowed: %v", err)
	}
	if err := message.ApplyTransition(msg, lifecycle.MsgSending, "", "", "", ""); err == nil {
		t.Fatal("delivered -> sending must be rejected")
	}
	if err := message.ApplyTransition(nil, lifecycle.MsgCreated, "", "", "", ""); err == nil {
		t.Fatal("nil message must error")
	}
}

func TestAggregateSegmentState(t *testing.T) {
	parts := func(states ...lifecycle.MessageState) []message.SegmentRecord {
		out := make([]message.SegmentRecord, 0, len(states))
		for i, s := range states {
			out = append(out, message.SegmentRecord{Index: i + 1, State: s})
		}
		return out
	}

	if got := message.AggregateSegmentState(nil); got != lifecycle.MsgUnknown {
		t.Fatalf("empty=%q want unknown", got)
	}
	if got := message.AggregateSegmentState(parts(lifecycle.MsgDelivered, lifecycle.MsgDelivered)); got != lifecycle.MsgDelivered {
		t.Fatalf("all delivered=%q", got)
	}
	if got := message.AggregateSegmentState(parts(lifecycle.MsgDelivered, lifecycle.MsgFailed)); got != lifecycle.MsgPartiallyDelivered {
		t.Fatalf("mixed terminal=%q want partially_delivered", got)
	}
	if got := message.AggregateSegmentState(parts(lifecycle.MsgFailed, lifecycle.MsgExpired)); got != lifecycle.MsgFailed {
		t.Fatalf("all failed=%q want failed", got)
	}
	if got := message.AggregateSegmentState(parts(lifecycle.MsgDelivered, lifecycle.MsgSending)); got != lifecycle.MsgPartiallyDelivered {
		t.Fatalf("partial progress=%q want partially_delivered", got)
	}
	if got := message.AggregateSegmentState(parts(lifecycle.MsgSubmitted, lifecycle.MsgSending)); got != lifecycle.MsgSubmitted {
		t.Fatalf("non-terminal=%q want submitted", got)
	}
	if got := message.AggregateSegmentState(parts(lifecycle.MsgRejected, lifecycle.MsgUndeliverable, lifecycle.MsgDeadLettered)); got != lifecycle.MsgFailed {
		t.Fatalf("all failure states=%q want failed", got)
	}
}
