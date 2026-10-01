package events

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Event struct {
	ID      string
	Topic   string
	At      time.Time
	TraceID string
	Data    any
}
type Handler func(context.Context, Event) error

// Store persists published events. Implementations may use memory, SQL,
// Redis, a broker, or any other backing store. Publish writes to the store
// before dispatching handlers, so successful publication is durable according
// to the selected store's contract.
type Store interface {
	Append(context.Context, Event) error
	List(context.Context, string, int) ([]Event, error)
}

// StoreFuncs adapts storage callbacks to Store.
type StoreFuncs struct {
	AppendFunc func(context.Context, Event) error
	ListFunc   func(context.Context, string, int) ([]Event, error)
}

func (s StoreFuncs) Append(ctx context.Context, e Event) error {
	if s.AppendFunc == nil {
		return fmt.Errorf("events: AppendFunc is required")
	}
	return s.AppendFunc(ctx, e)
}

func (s StoreFuncs) List(ctx context.Context, topic string, limit int) ([]Event, error) {
	if s.ListFunc == nil {
		return nil, fmt.Errorf("events: ListFunc is required")
	}
	return s.ListFunc(ctx, topic, limit)
}

type Bus interface {
	Publish(context.Context, Event) error
	Subscribe(topic string, h Handler) func()
}

type MemoryBus struct {
	mu    sync.RWMutex
	subs  map[string]map[uint64]Handler
	next  uint64
	async bool
	store Store
}

func NewMemoryBus(async bool) *MemoryBus {
	return NewBus(async, nil)
}

// NewBus creates the default in-memory bus with an optional event store. A nil
// store uses MemoryEventStore, so existing callers remain fully in-memory.
func NewBus(async bool, store Store) *MemoryBus {
	if store == nil {
		store = NewMemoryEventStore(10000)
	}
	return &MemoryBus{subs: map[string]map[uint64]Handler{}, async: async, store: store}
}

// SetStore replaces the event store for future publications. Configure this
// before concurrent publishing begins.
func (b *MemoryBus) SetStore(store Store) {
	if store == nil {
		store = NewMemoryEventStore(10000)
	}
	b.mu.Lock()
	b.store = store
	b.mu.Unlock()
}

func (b *MemoryBus) Store() Store {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.store
}
func (b *MemoryBus) Subscribe(topic string, h Handler) func() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.next++
	id := b.next
	if b.subs[topic] == nil {
		b.subs[topic] = map[uint64]Handler{}
	}
	b.subs[topic][id] = h
	return func() { b.mu.Lock(); delete(b.subs[topic], id); b.mu.Unlock() }
}
func (b *MemoryBus) Publish(ctx context.Context, e Event) error {
	if e.At.IsZero() {
		e.At = time.Now()
	}
	b.mu.RLock()
	store := b.store
	var hs []Handler
	for pattern, handlers := range b.subs {
		if TopicMatch(pattern, e.Topic) {
			for _, h := range handlers {
				hs = append(hs, h)
			}
		}
	}
	b.mu.RUnlock()
	if e.ID == "" {
		e.ID = e.Topic + "-" + fmt.Sprintf("%d", e.At.UnixNano())
	}
	if err := store.Append(ctx, e); err != nil {
		return err
	}
	for _, h := range hs {
		hh := h
		if b.async {
			go func() { _ = hh(ctx, e) }()
		} else if err := hh(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// TopicMatch implements AMQP-style topic patterns: * matches one dot-delimited
// segment and # matches zero or more segments.
func TopicMatch(pattern, topic string) bool {
	if pattern == "" || pattern == topic {
		return true
	}
	pp := strings.Split(pattern, ".")
	tt := strings.Split(topic, ".")
	return matchTopicParts(pp, tt)
}

func matchTopicParts(pattern, topic []string) bool {
	for len(pattern) > 0 {
		part := pattern[0]
		pattern = pattern[1:]
		if part == "#" {
			if len(pattern) == 0 {
				return true
			}
			for i := 0; i <= len(topic); i++ {
				if matchTopicParts(pattern, topic[i:]) {
					return true
				}
			}
			return false
		}
		if len(topic) == 0 || (part != "*" && part != topic[0]) {
			return false
		}
		topic = topic[1:]
	}
	return len(topic) == 0
}

const (
	ConnectionOpened = "connection.opened"
	ConnectionClosed = "connection.closed"
	BindSucceeded    = "bind.succeeded"
	BindFailed       = "bind.failed"
	PduSent          = "pdu.sent"
	PduReceived      = "pdu.received"
	MessageCreated   = "message.created"
	MessageSegmented = "message.segmented"
	MessageSubmitted = "message.submitted"
	MessageDelivered = "message.delivered"
	DLRReceived      = "dlr.received"
	InboundReceived  = "inbound.received"
	HookFailed       = "hook.failed"
)
