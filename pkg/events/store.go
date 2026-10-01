package events

import (
	"context"
	"sync"
	"time"
)

// MemoryEventStore is the default bounded in-memory event store. It is safe
// for concurrent publishers and readers; applications needing persistence can
// implement Store or use StoreFuncs with their database/queue of choice.
type MemoryEventStore struct {
	mu       sync.RWMutex
	capacity int
	events   []Event
}

func NewMemoryEventStore(capacity int) *MemoryEventStore {
	if capacity <= 0 {
		capacity = 10000
	}
	return &MemoryEventStore{capacity: capacity}
}

func (s *MemoryEventStore) Append(ctx context.Context, e Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.At.IsZero() {
		e.At = time.Now()
	}
	s.events = append(s.events, e)
	if len(s.events) > s.capacity {
		start := len(s.events) - s.capacity
		s.events = append([]Event(nil), s.events[start:]...)
	}
	return nil
}

func (s *MemoryEventStore) List(ctx context.Context, topic string, limit int) ([]Event, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Event, 0, len(s.events))
	for _, event := range s.events {
		if topic != "" && !TopicMatch(topic, event.Topic) {
			continue
		}
		out = append(out, event)
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, nil
}
