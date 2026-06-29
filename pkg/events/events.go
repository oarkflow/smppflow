package events

import (
	"context"
	"sync"
	"time"
)

type Event struct {
	Topic   string
	At      time.Time
	TraceID string
	Data    any
}
type Handler func(context.Context, Event) error
type Bus interface {
	Publish(context.Context, Event) error
	Subscribe(topic string, h Handler) func()
}

type MemoryBus struct {
	mu    sync.RWMutex
	subs  map[string]map[uint64]Handler
	next  uint64
	async bool
}

func NewMemoryBus(async bool) *MemoryBus {
	return &MemoryBus{subs: map[string]map[uint64]Handler{}, async: async}
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
	var hs []Handler
	for _, topic := range []string{e.Topic, "*"} {
		for _, h := range b.subs[topic] {
			hs = append(hs, h)
		}
	}
	b.mu.RUnlock()
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
