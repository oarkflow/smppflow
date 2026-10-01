package events

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestTopicMatch(t *testing.T) {
	cases := []struct {
		pattern string
		topic   string
		want    bool
	}{
		{"sms.*", "sms.sent", true},
		{"sms.*", "sms.provider.sent", false},
		{"sms.#", "sms.provider.sent", true},
		{"sms.#", "sms", true},
		{"#", "sms.provider.sent", true},
		{"sms.sent", "sms.sent", true},
		{"sms.sent", "sms.failed", false},
	}
	for _, tc := range cases {
		if got := TopicMatch(tc.pattern, tc.topic); got != tc.want {
			t.Errorf("TopicMatch(%q, %q)=%v want %v", tc.pattern, tc.topic, got, tc.want)
		}
	}
}

func TestMemoryBusPersistsBeforeDispatchAndSupportsPatterns(t *testing.T) {
	store := NewMemoryEventStore(10)
	bus := NewBus(false, store)
	var mu sync.Mutex
	seen := []string{}
	bus.Subscribe("sms.#", func(ctx context.Context, e Event) error {
		stored, err := store.List(ctx, e.Topic, 1)
		if err != nil {
			return err
		}
		if len(stored) != 1 || stored[0].ID != e.ID {
			return errors.New("event was dispatched before persistence")
		}
		mu.Lock()
		seen = append(seen, e.Topic)
		mu.Unlock()
		return nil
	})
	if err := bus.Publish(context.Background(), Event{Topic: "sms.provider.sent", Data: "ok"}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), Event{Topic: "billing.created"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0] != "sms.provider.sent" {
		t.Fatalf("seen=%v want only matching event", seen)
	}
	all, err := store.List(context.Background(), "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("stored events=%d want 2", len(all))
	}
}

func TestMemoryEventStoreIsBounded(t *testing.T) {
	store := NewMemoryEventStore(2)
	ctx := context.Background()
	for _, topic := range []string{"one", "two", "three"} {
		if err := store.Append(ctx, Event{Topic: topic}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.List(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Topic != "two" || got[1].Topic != "three" {
		t.Fatalf("got=%v want two,three", got)
	}
}

func TestBusStoreAdapter(t *testing.T) {
	var stored Event
	bus := NewBus(false, StoreFuncs{
		AppendFunc: func(ctx context.Context, e Event) error { stored = e; return nil },
		ListFunc:   func(context.Context, string, int) ([]Event, error) { return []Event{stored}, nil },
	})
	if err := bus.Publish(context.Background(), Event{Topic: "custom.storage"}); err != nil {
		t.Fatal(err)
	}
	if stored.Topic != "custom.storage" {
		t.Fatalf("stored topic=%q", stored.Topic)
	}
}
