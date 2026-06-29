package message

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/oarkflow/smppflow/pkg/events"
	"github.com/oarkflow/smppflow/pkg/hooks"
	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/session"
)

type Transport interface {
	SubmitSM(context.Context, protocol.SubmitSMRequest) (protocol.SubmitSMRespData, error)
	Inbound() <-chan protocol.PDU
}

type Store interface {
	SaveMessage(context.Context, *Message) error
	GetMessage(context.Context, ID) (*Message, error)
	ListMessages(context.Context) ([]*Message, error)
	UpdateMessageState(context.Context, ID, lifecycle.MessageState) error
	SaveDLR(context.Context, *DeliveryReceipt) error
	ListDLR(context.Context) ([]*DeliveryReceipt, error)
}

type ManagerConfig struct {
	Client           *session.Client
	Store            Store
	Bus              events.Bus
	Hooks            *hooks.Manager
	DefaultSourceTON protocol.TON
	DefaultSourceNPI protocol.NPI
	DefaultDestTON   protocol.TON
	DefaultDestNPI   protocol.NPI
	UseSAR           bool
}
type Manager struct {
	cfg  ManagerConfig
	ids  atomic.Uint64
	stop chan struct{}
}

func NewManager(cfg ManagerConfig) *Manager {
	if cfg.Store == nil {
		panic("smppflow/message: Store is required")
	}
	if cfg.Bus == nil {
		cfg.Bus = events.NewMemoryBus(true)
	}
	if cfg.Hooks == nil {
		cfg.Hooks = hooks.New()
	}
	if cfg.DefaultDestNPI == 0 {
		cfg.DefaultDestNPI = protocol.NPIISDN
	}
	return &Manager{cfg: cfg, stop: make(chan struct{})}
}
func (m *Manager) nextID() ID {
	return ID(fmt.Sprintf("msg-%d-%d", time.Now().UnixNano(), m.ids.Add(1)))
}
func (m *Manager) Submit(ctx context.Context, req SubmitRequest) (SubmitResult, error) {
	if m.cfg.Client == nil {
		return SubmitResult{}, fmt.Errorf("nil smpp client")
	}
	from := protocol.AutoDetectAddress(protocol.Address{TON: m.cfg.DefaultSourceTON, NPI: m.cfg.DefaultSourceNPI, Addr: req.From})
	to := protocol.AutoDetectAddress(protocol.Address{TON: m.cfg.DefaultDestTON, NPI: m.cfg.DefaultDestNPI, Addr: req.To})
	msg := &Message{ID: m.nextID(), TenantID: req.TenantID, From: from, To: to, Text: req.Text, RegisteredDelivery: req.RegisteredDelivery, State: lifecycle.MsgCreated, CreatedAt: time.Now(), UpdatedAt: time.Now(), Meta: req.Meta}
	_ = m.cfg.Bus.Publish(ctx, events.Event{Topic: events.MessageCreated, Data: msg.ID})
	_ = m.cfg.Hooks.Run(ctx, hooks.BeforeMessageValidate, msg)
	if msg.To.Addr == "" || msg.From.Addr == "" {
		return SubmitResult{}, fmt.Errorf("source and destination are required")
	}
	_ = m.cfg.Hooks.Run(ctx, hooks.AfterMessageValidate, msg)
	_ = m.cfg.Hooks.Run(ctx, hooks.BeforeSegment, msg)
	segs, analysis, err := SegmentText(req.Text, SegmentPolicy{UseSAR: req.UseSAR || m.cfg.UseSAR})
	if err != nil {
		return SubmitResult{}, err
	}
	msg.Encoding = analysis.Encoding
	msg.DataCoding = analysis.DataCoding
	msg.State = lifecycle.MsgSegmented
	for _, s := range segs {
		msg.Parts = append(msg.Parts, SegmentRecord{ID: fmt.Sprintf("%s-%d", msg.ID, s.Index), Index: s.Index, Total: s.Total, Ref: s.Ref, Payload: s.Payload, DataCoding: s.DataCoding, State: lifecycle.MsgCreated, CreatedAt: time.Now(), UpdatedAt: time.Now()})
	}
	_ = m.cfg.Hooks.Run(ctx, hooks.AfterSegment, msg)
	_ = m.cfg.Bus.Publish(ctx, events.Event{Topic: events.MessageSegmented, Data: msg.ID})
	if err := m.cfg.Store.SaveMessage(ctx, msg); err != nil {
		return SubmitResult{}, err
	}
	ids := make([]string, 0, len(segs))
	for i, s := range segs {
		_ = m.cfg.Hooks.Run(ctx, hooks.BeforeSubmit, s)
		resp, err := m.cfg.Client.SubmitSM(ctx, protocol.SubmitSMRequest{Source: msg.From, Destination: msg.To, ESMClass: s.ESMClass, RegisteredDelivery: req.RegisteredDelivery, DataCoding: s.DataCoding, ShortMessage: s.Payload, TLVs: s.TLVs})
		if err != nil {
			msg.Parts[i].State = lifecycle.MsgFailed
			msg.State = lifecycle.MsgFailed
			_ = m.cfg.Store.SaveMessage(ctx, msg)
			return SubmitResult{MessageID: msg.ID, ProviderIDs: ids, Parts: len(segs), State: msg.State}, err
		}
		ids = append(ids, resp.MessageID)
		msg.Parts[i].ProviderMessageID = resp.MessageID
		msg.Parts[i].State = lifecycle.MsgSubmitted
		if i == 0 {
			msg.ProviderMessageID = resp.MessageID
		}
		_ = m.cfg.Hooks.Run(ctx, hooks.AfterSubmit, resp)
		_ = m.cfg.Bus.Publish(ctx, events.Event{Topic: events.MessageSubmitted, Data: resp.MessageID})
	}
	msg.State = lifecycle.MsgSubmitted
	msg.UpdatedAt = time.Now()
	_ = m.cfg.Store.SaveMessage(ctx, msg)
	return SubmitResult{MessageID: msg.ID, ProviderIDs: ids, Parts: len(segs), State: msg.State}, nil
}

func (m *Manager) StartInbound(ctx context.Context) {
	if m.cfg.Client == nil {
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case p := <-m.cfg.Client.Inbound():
				if p.CommandID == protocol.DeliverSM {
					req, err := protocol.ParseSubmitSM(p)
					if err != nil {
						continue
					}
					if req.ESMClass&protocol.ESMClassDeliveryReceipt != 0 {
						d := ParseDLR(string(req.ShortMessage), req.TLVs)
						_ = m.cfg.Hooks.Run(ctx, hooks.OnDLRReceived, d)
						_ = m.cfg.Store.SaveDLR(ctx, &d)
						_ = m.cfg.Bus.Publish(ctx, events.Event{Topic: events.DLRReceived, Data: d})
					} else {
						_ = m.cfg.Bus.Publish(ctx, events.Event{Topic: events.InboundReceived, Data: req})
					}
				}
			}
		}
	}()
}
