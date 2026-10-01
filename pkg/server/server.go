package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/smppflow/pkg/codec"
	"github.com/oarkflow/smppflow/pkg/events"
	"github.com/oarkflow/smppflow/pkg/protocol"
)

type AuthFunc func(protocol.BindRequest) protocol.CommandStatus
type SubmitHandler func(context.Context, protocol.SubmitSMRequest) (string, protocol.CommandStatus)
type QueryHandler func(context.Context, protocol.QuerySMRequest) (protocol.QuerySMRespData, protocol.CommandStatus)
type BroadcastQueryHandler func(context.Context, protocol.QueryBroadcastSMRequest) (protocol.QueryBroadcastSMRespData, protocol.CommandStatus)
type SimpleHandler func(context.Context, any) protocol.CommandStatus

type Config struct {
	Addr            string
	TLS             *tls.Config
	SystemID        string
	Version         protocol.Version
	Auth            AuthFunc
	Submit          SubmitHandler
	Bus             events.Bus
	AutoDLR         bool
	DLRDelay        time.Duration
	Query           QueryHandler
	Cancel          SimpleHandler
	Replace         SimpleHandler
	DataSM          SubmitHandler
	Broadcast       SubmitHandler
	QueryBroadcast  BroadcastQueryHandler
	CancelBroadcast SimpleHandler
}
type Server struct {
	cfg       Config
	mu        sync.RWMutex
	ln        net.Listener
	closed    bool
	ready     chan struct{}
	readyOnce sync.Once
	seq       atomic.Uint32
}

func New(cfg Config) *Server {
	if cfg.Version == 0 {
		cfg.Version = protocol.Version34
	}
	if cfg.Bus == nil {
		cfg.Bus = events.NewMemoryBus(true)
	}
	if cfg.Auth == nil {
		cfg.Auth = func(protocol.BindRequest) protocol.CommandStatus { return protocol.ESME_ROK }
	}
	if cfg.Submit == nil {
		cfg.Submit = func(ctx context.Context, req protocol.SubmitSMRequest) (string, protocol.CommandStatus) {
			return fmt.Sprintf("%d", time.Now().UnixNano()), protocol.ESME_ROK
		}
	}
	if cfg.DLRDelay == 0 {
		cfg.DLRDelay = 200 * time.Millisecond
	}
	if cfg.Query == nil {
		cfg.Query = func(ctx context.Context, req protocol.QuerySMRequest) (protocol.QuerySMRespData, protocol.CommandStatus) {
			return protocol.QuerySMRespData{MessageID: req.MessageID, MessageState: 2, ErrorCode: 0}, protocol.ESME_ROK
		}
	}
	if cfg.Cancel == nil {
		cfg.Cancel = func(ctx context.Context, req any) protocol.CommandStatus { return protocol.ESME_ROK }
	}
	if cfg.Replace == nil {
		cfg.Replace = func(ctx context.Context, req any) protocol.CommandStatus { return protocol.ESME_ROK }
	}
	if cfg.DataSM == nil {
		cfg.DataSM = cfg.Submit
	}
	if cfg.Broadcast == nil {
		cfg.Broadcast = cfg.Submit
	}
	if cfg.QueryBroadcast == nil {
		cfg.QueryBroadcast = func(ctx context.Context, req protocol.QueryBroadcastSMRequest) (protocol.QueryBroadcastSMRespData, protocol.CommandStatus) {
			return protocol.QueryBroadcastSMRespData{MessageID: req.MessageID, MessageState: 2}, protocol.ESME_ROK
		}
	}
	if cfg.CancelBroadcast == nil {
		cfg.CancelBroadcast = func(ctx context.Context, req any) protocol.CommandStatus { return protocol.ESME_ROK }
	}
	return &Server{cfg: cfg, ready: make(chan struct{})}
}

// ListenAndServe binds the configured address and serves SMPP PDUs until ctx is
// cancelled or the listener is closed. The bound address is published via Ready
// and Addr as soon as the listener is established.
func (s *Server) ListenAndServe(ctx context.Context) error {
	var ln net.Listener
	var err error
	if s.cfg.TLS != nil {
		ln, err = tls.Listen("tcp", s.cfg.Addr, s.cfg.TLS)
	} else {
		ln, err = net.Listen("tcp", s.cfg.Addr)
	}
	if err != nil {
		s.markReady()
		return err
	}
	s.setListener(ln)
	go func() { <-ctx.Done(); _ = ln.Close() }()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handle(ctx, conn)
	}
}

// Ready returns a channel closed once the listener has been bound (or binding
// failed). Callers that need the concrete bound address should wait on it
// before calling Addr, instead of sleeping.
func (s *Server) Ready() <-chan struct{} { return s.ready }

// Addr reports the bound listener address. Before the listener exists it
// returns the configured address, which is unchanged if Addr was not ":0".
func (s *Server) Addr() string {
	s.mu.RLock()
	ln := s.ln
	s.mu.RUnlock()
	if ln == nil {
		return s.cfg.Addr
	}
	return ln.Addr().String()
}

// Close stops the listener. It is safe to call concurrently with ListenAndServe
// and is idempotent: repeated calls return nil.
func (s *Server) Close() error {
	s.mu.Lock()
	ln := s.ln
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	return nil
}

func (s *Server) setListener(ln net.Listener) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = ln.Close()
		s.markReady()
		return
	}
	s.ln = ln
	s.mu.Unlock()
	s.markReady()
}

func (s *Server) markReady() { s.readyOnce.Do(func() { close(s.ready) }) }
func (s *Server) nextSeq() uint32 {
	n := s.seq.Add(1)
	if n == 0 {
		n = s.seq.Add(1)
	}
	return n
}
func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	bound := false
	sessVersion := s.cfg.Version
	for {
		p, err := codec.ReadPDU(conn)
		if err != nil {
			return
		}
		_ = s.cfg.Bus.Publish(ctx, events.Event{Topic: events.PduReceived, Data: p.CommandID.String()})
		if bound && !protocol.SupportsCommand(sessVersion, p.CommandID) {
			_ = codec.WritePDU(conn, protocol.BuildGenericNACK(p.SequenceNumber, protocol.ESME_RINVCMDID))
			continue
		}
		switch p.CommandID {
		case protocol.BindReceiver, protocol.BindTransmitter, protocol.BindTransceiver:
			req, err := protocol.ParseBind(p)
			st := protocol.ESME_ROK
			if err != nil {
				st = protocol.ESME_RINVCMDLEN
			} else {
				st = s.cfg.Auth(req)
			}
			if st == protocol.ESME_ROK {
				bound = true
				sessVersion = req.InterfaceVersion
				if sessVersion == 0 {
					sessVersion = s.cfg.Version
				}
			}
			_ = codec.WritePDU(conn, protocol.BuildBindResp(p.SequenceNumber, p.CommandID, st, s.cfg.SystemID, s.cfg.Version))
		case protocol.EnquireLink:
			// Keepalive is legal before and after bind.
			_ = codec.WritePDU(conn, protocol.BuildEnquireLinkResp(p.SequenceNumber))
		case protocol.Unbind:
			_ = codec.WritePDU(conn, protocol.BuildUnbindResp(p.SequenceNumber))
			return
		case protocol.SubmitSM, protocol.DataSM, protocol.QuerySM, protocol.CancelSM,
			protocol.ReplaceSM, protocol.SubmitMulti, protocol.BroadcastSM,
			protocol.QueryBroadcastSM, protocol.CancelBroadcastSM:
			// Every session command requires a successful bind; otherwise an
			// unauthenticated peer could query, cancel or replace messages
			// without ever authenticating. The spec expects the matching *_resp
			// PDU carrying ESME_RINVBNDSTS rather than a generic NACK.
			if !bound {
				_ = codec.WritePDU(conn, buildBindRequiredResp(p.SequenceNumber, p.CommandID))
				continue
			}
			s.handleSessionCommand(ctx, conn, p)
		case protocol.AlertNotification, protocol.Outbind:
			// These commands are SMSC initiated in normal deployments. When received by the mock
			// server we accept the PDU at transport level and publish it for tests/hooks.
		case protocol.DeliverSMResp:
		default:
			_ = codec.WritePDU(conn, protocol.BuildGenericNACK(p.SequenceNumber, protocol.ESME_RINVCMDID))
		}
	}
}

// buildBindRequiredResp answers a session command that arrived before bind with
// the response PDU type matching the request, carrying ESME_RINVBNDSTS.
func buildBindRequiredResp(seq uint32, cmd protocol.CommandID) protocol.PDU {
	switch cmd {
	case protocol.SubmitSM, protocol.BroadcastSM:
		return protocol.BuildSubmitSMResp(seq, protocol.ESME_RINVBNDSTS, "")
	case protocol.SubmitMulti:
		return protocol.BuildSubmitMultiResp(seq, protocol.ESME_RINVBNDSTS, "")
	case protocol.DataSM:
		return protocol.BuildDataSMResp(seq, protocol.ESME_RINVBNDSTS, "")
	case protocol.QuerySM:
		return protocol.BuildQuerySMResp(seq, protocol.ESME_RINVBNDSTS, protocol.QuerySMRespData{})
	case protocol.CancelSM:
		return protocol.BuildCancelSMResp(seq, protocol.ESME_RINVBNDSTS)
	case protocol.ReplaceSM:
		return protocol.BuildReplaceSMResp(seq, protocol.ESME_RINVBNDSTS)
	case protocol.QueryBroadcastSM:
		return protocol.BuildQueryBroadcastSMResp(seq, protocol.ESME_RINVBNDSTS, protocol.QueryBroadcastSMRespData{})
	case protocol.CancelBroadcastSM:
		return protocol.BuildCancelBroadcastSMResp(seq, protocol.ESME_RINVBNDSTS)
	default:
		return protocol.BuildGenericNACK(seq, protocol.ESME_RINVBNDSTS)
	}
}

// handleSessionCommand dispatches a PDU that requires a bound session. The
// caller has already verified the session is bound.
func (s *Server) handleSessionCommand(ctx context.Context, conn net.Conn, p protocol.PDU) {
	switch p.CommandID {
	case protocol.SubmitSM:
		req, err := protocol.ParseSubmitSM(p)
		if err != nil {
			_ = codec.WritePDU(conn, protocol.BuildSubmitSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN, ""))
			return
		}
		id, st := s.cfg.Submit(ctx, req)
		_ = codec.WritePDU(conn, protocol.BuildSubmitSMResp(p.SequenceNumber, st, id))
		if st == protocol.ESME_ROK && s.cfg.AutoDLR && req.RegisteredDelivery != 0 {
			go s.sendDLR(conn, req, id)
		}
	case protocol.DataSM:
		req, err := protocol.ParseSubmitSM(p)
		if err != nil {
			_ = codec.WritePDU(conn, protocol.BuildDataSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN, ""))
			return
		}
		id, st := s.cfg.DataSM(ctx, req)
		_ = codec.WritePDU(conn, protocol.BuildDataSMResp(p.SequenceNumber, st, id))
	case protocol.QuerySM:
		req, err := protocol.ParseQuerySM(p)
		if err != nil {
			_ = codec.WritePDU(conn, protocol.BuildQuerySMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN, protocol.QuerySMRespData{}))
			return
		}
		data, st := s.cfg.Query(ctx, req)
		_ = codec.WritePDU(conn, protocol.BuildQuerySMResp(p.SequenceNumber, st, data))
	case protocol.CancelSM:
		req, err := protocol.ParseCancelSM(p)
		if err != nil {
			_ = codec.WritePDU(conn, protocol.BuildCancelSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN))
			return
		}
		_ = codec.WritePDU(conn, protocol.BuildCancelSMResp(p.SequenceNumber, s.cfg.Cancel(ctx, req)))
	case protocol.ReplaceSM:
		req, err := protocol.ParseReplaceSM(p)
		if err != nil {
			_ = codec.WritePDU(conn, protocol.BuildReplaceSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN))
			return
		}
		_ = codec.WritePDU(conn, protocol.BuildReplaceSMResp(p.SequenceNumber, s.cfg.Replace(ctx, req)))
	case protocol.SubmitMulti:
		_ = codec.WritePDU(conn, protocol.BuildSubmitMultiResp(p.SequenceNumber, protocol.ESME_ROK, fmt.Sprintf("%d", time.Now().UnixNano())))
	case protocol.BroadcastSM:
		req, err := protocol.ParseBroadcastSM(p)
		if err != nil {
			_ = codec.WritePDU(conn, protocol.BuildBroadcastSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN, ""))
			return
		}
		id, st := s.cfg.Broadcast(ctx, protocol.SubmitSMRequest{ServiceType: req.ServiceType, Source: req.Source, Destination: protocol.Address{}, DataCoding: req.DataCoding, ShortMessage: req.ShortMessage, TLVs: req.TLVs})
		_ = codec.WritePDU(conn, protocol.BuildBroadcastSMResp(p.SequenceNumber, st, id))
	case protocol.QueryBroadcastSM:
		req, err := protocol.ParseQueryBroadcastSM(p)
		if err != nil {
			_ = codec.WritePDU(conn, protocol.BuildQueryBroadcastSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN, protocol.QueryBroadcastSMRespData{}))
			return
		}
		data, st := s.cfg.QueryBroadcast(ctx, req)
		_ = codec.WritePDU(conn, protocol.BuildQueryBroadcastSMResp(p.SequenceNumber, st, data))
	case protocol.CancelBroadcastSM:
		req, err := protocol.ParseCancelBroadcastSM(p)
		if err != nil {
			_ = codec.WritePDU(conn, protocol.BuildCancelBroadcastSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN))
			return
		}
		_ = codec.WritePDU(conn, protocol.BuildCancelBroadcastSMResp(p.SequenceNumber, s.cfg.CancelBroadcast(ctx, req)))
	}
}
func (s *Server) sendDLR(conn net.Conn, req protocol.SubmitSMRequest, id string) {
	time.Sleep(s.cfg.DLRDelay)
	body := fmt.Sprintf("id:%s sub:001 dlvrd:001 submit date:%s done date:%s stat:DELIVRD err:000 text:", id, time.Now().Format("0601021504"), time.Now().Format("0601021504"))
	dlr := protocol.SubmitSMRequest{Source: req.Destination, Destination: req.Source, ESMClass: protocol.ESMClassDeliveryReceipt, DataCoding: protocol.DataCodingDefault, ShortMessage: []byte(body), TLVs: []protocol.TLV{protocol.TLVCString(protocol.TLVReceiptedMessageID, id), protocol.TLVByte(protocol.TLVMessageState, 2)}}
	_ = codec.WritePDU(conn, protocol.BuildDeliverSM(s.nextSeq(), dlr))
}
