package storage

import (
	"context"
	"sync"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
)

type Store interface {
	SaveMessage(context.Context, *message.Message) error
	GetMessage(context.Context, message.ID) (*message.Message, error)
	ListMessages(context.Context) ([]*message.Message, error)
	UpdateMessageState(context.Context, message.ID, lifecycle.MessageState) error
	SaveDLR(context.Context, *message.DeliveryReceipt) error
	ListDLR(context.Context) ([]*message.DeliveryReceipt, error)
}

type Memory struct {
	mu   sync.RWMutex
	msgs map[message.ID]*message.Message
	dlrs []*message.DeliveryReceipt
}

func NewMemory() *Memory { return &Memory{msgs: map[message.ID]*message.Message{}} }
func (m *Memory) SaveMessage(ctx context.Context, msg *message.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *msg
	cp.Parts = append([]message.SegmentRecord(nil), msg.Parts...)
	m.msgs[msg.ID] = &cp
	return nil
}
func (m *Memory) GetMessage(ctx context.Context, id message.ID) (*message.Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	x := m.msgs[id]
	if x == nil {
		return nil, ErrNotFound
	}
	cp := *x
	cp.Parts = append([]message.SegmentRecord(nil), x.Parts...)
	return &cp, nil
}
func (m *Memory) ListMessages(ctx context.Context) ([]*message.Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*message.Message, 0, len(m.msgs))
	for _, x := range m.msgs {
		cp := *x
		cp.Parts = append([]message.SegmentRecord(nil), x.Parts...)
		out = append(out, &cp)
	}
	return out, nil
}
func (m *Memory) UpdateMessageState(ctx context.Context, id message.ID, st lifecycle.MessageState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	x := m.msgs[id]
	if x == nil {
		return ErrNotFound
	}
	x.State = st
	return nil
}
func (m *Memory) SaveDLR(ctx context.Context, d *message.DeliveryReceipt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *d
	m.dlrs = append(m.dlrs, &cp)
	for _, msg := range m.msgs {
		if msg.ProviderMessageID == d.ProviderMessageID || string(msg.ID) == d.MessageID {
			msg.State = d.State
		}
	}
	return nil
}
func (m *Memory) ListDLR(ctx context.Context) ([]*message.DeliveryReceipt, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*message.DeliveryReceipt, 0, len(m.dlrs))
	for _, d := range m.dlrs {
		cp := *d
		out = append(out, &cp)
	}
	return out, nil
}

var ErrNotFound = errString("not found")

type errString string

func (e errString) Error() string { return string(e) }
