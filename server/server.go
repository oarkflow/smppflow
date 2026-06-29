package server

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/oarkflow/smppflow/codec"
	"github.com/oarkflow/smppflow/events"
	"github.com/oarkflow/smppflow/protocol"
)

type AuthFunc func(protocol.BindRequest) protocol.CommandStatus
type SubmitHandler func(context.Context, protocol.SubmitSMRequest) (string, protocol.CommandStatus)
type QueryHandler func(context.Context, protocol.QuerySMRequest) (protocol.QuerySMRespData, protocol.CommandStatus)
type BroadcastQueryHandler func(context.Context, protocol.QueryBroadcastSMRequest) (protocol.QueryBroadcastSMRespData, protocol.CommandStatus)
type SimpleHandler func(context.Context, any) protocol.CommandStatus

type Config struct {
	Addr            string
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
	cfg Config
	ln  net.Listener
	seq atomic.Uint32
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
	return &Server{cfg: cfg}
}
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.ln = ln
	go func() { <-ctx.Done(); _ = ln.Close() }()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handle(ctx, conn)
	}
}
func (s *Server) Addr() string {
	if s.ln == nil {
		return s.cfg.Addr
	}
	return s.ln.Addr().String()
}
func (s *Server) Close() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}
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
			_ = codec.WritePDU(conn, protocol.BuildEnquireLinkResp(p.SequenceNumber))
		case protocol.Unbind:
			_ = codec.WritePDU(conn, protocol.BuildUnbindResp(p.SequenceNumber))
			return
		case protocol.SubmitSM:
			if !bound {
				_ = codec.WritePDU(conn, protocol.BuildSubmitSMResp(p.SequenceNumber, protocol.ESME_RINVBNDSTS, ""))
				continue
			}
			req, err := protocol.ParseSubmitSM(p)
			if err != nil {
				_ = codec.WritePDU(conn, protocol.BuildSubmitSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN, ""))
				continue
			}
			id, st := s.cfg.Submit(ctx, req)
			_ = codec.WritePDU(conn, protocol.BuildSubmitSMResp(p.SequenceNumber, st, id))
			if st == protocol.ESME_ROK && s.cfg.AutoDLR && req.RegisteredDelivery != 0 {
				go s.sendDLR(conn, req, id)
			}
		case protocol.DataSM:
			if !bound {
				_ = codec.WritePDU(conn, protocol.BuildDataSMResp(p.SequenceNumber, protocol.ESME_RINVBNDSTS, ""))
				continue
			}
			req, err := protocol.ParseSubmitSM(p)
			if err != nil {
				_ = codec.WritePDU(conn, protocol.BuildDataSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN, ""))
				continue
			}
			id, st := s.cfg.DataSM(ctx, req)
			_ = codec.WritePDU(conn, protocol.BuildDataSMResp(p.SequenceNumber, st, id))
		case protocol.QuerySM:
			req, err := protocol.ParseQuerySM(p)
			if err != nil {
				_ = codec.WritePDU(conn, protocol.BuildQuerySMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN, protocol.QuerySMRespData{}))
				continue
			}
			data, st := s.cfg.Query(ctx, req)
			_ = codec.WritePDU(conn, protocol.BuildQuerySMResp(p.SequenceNumber, st, data))
		case protocol.CancelSM:
			req, err := protocol.ParseCancelSM(p)
			if err != nil {
				_ = codec.WritePDU(conn, protocol.BuildCancelSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN))
				continue
			}
			_ = codec.WritePDU(conn, protocol.BuildCancelSMResp(p.SequenceNumber, s.cfg.Cancel(ctx, req)))
		case protocol.ReplaceSM:
			req, err := protocol.ParseReplaceSM(p)
			if err != nil {
				_ = codec.WritePDU(conn, protocol.BuildReplaceSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN))
				continue
			}
			_ = codec.WritePDU(conn, protocol.BuildReplaceSMResp(p.SequenceNumber, s.cfg.Replace(ctx, req)))
		case protocol.SubmitMulti:
			_ = codec.WritePDU(conn, protocol.BuildSubmitMultiResp(p.SequenceNumber, protocol.ESME_ROK, fmt.Sprintf("%d", time.Now().UnixNano())))
		case protocol.BroadcastSM:
			req, err := protocol.ParseBroadcastSM(p)
			if err != nil {
				_ = codec.WritePDU(conn, protocol.BuildBroadcastSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN, ""))
				continue
			}
			id, st := s.cfg.Broadcast(ctx, protocol.SubmitSMRequest{ServiceType: req.ServiceType, Source: req.Source, Destination: protocol.Address{}, DataCoding: req.DataCoding, ShortMessage: req.ShortMessage, TLVs: req.TLVs})
			_ = codec.WritePDU(conn, protocol.BuildBroadcastSMResp(p.SequenceNumber, st, id))
		case protocol.QueryBroadcastSM:
			req, err := protocol.ParseQueryBroadcastSM(p)
			if err != nil {
				_ = codec.WritePDU(conn, protocol.BuildQueryBroadcastSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN, protocol.QueryBroadcastSMRespData{}))
				continue
			}
			data, st := s.cfg.QueryBroadcast(ctx, req)
			_ = codec.WritePDU(conn, protocol.BuildQueryBroadcastSMResp(p.SequenceNumber, st, data))
		case protocol.CancelBroadcastSM:
			req, err := protocol.ParseCancelBroadcastSM(p)
			if err != nil {
				_ = codec.WritePDU(conn, protocol.BuildCancelBroadcastSMResp(p.SequenceNumber, protocol.ESME_RINVMSGLEN))
				continue
			}
			_ = codec.WritePDU(conn, protocol.BuildCancelBroadcastSMResp(p.SequenceNumber, s.cfg.CancelBroadcast(ctx, req)))
		case protocol.AlertNotification, protocol.Outbind:
			// These commands are SMSC initiated in normal deployments. When received by the mock
			// server we accept the PDU at transport level and publish it for tests/hooks.
		case protocol.DeliverSMResp:
		default:
			_ = codec.WritePDU(conn, protocol.BuildGenericNACK(p.SequenceNumber, protocol.ESME_RINVCMDID))
		}
	}
}
func (s *Server) sendDLR(conn net.Conn, req protocol.SubmitSMRequest, id string) {
	time.Sleep(s.cfg.DLRDelay)
	body := fmt.Sprintf("id:%s sub:001 dlvrd:001 submit date:%s done date:%s stat:DELIVRD err:000 text:", id, time.Now().Format("0601021504"), time.Now().Format("0601021504"))
	dlr := protocol.SubmitSMRequest{Source: req.Destination, Destination: req.Source, ESMClass: protocol.ESMClassDeliveryReceipt, DataCoding: protocol.DataCodingDefault, ShortMessage: []byte(body), TLVs: []protocol.TLV{protocol.TLVCString(protocol.TLVReceiptedMessageID, id), protocol.TLVByte(protocol.TLVMessageState, 2)}}
	_ = codec.WritePDU(conn, protocol.BuildDeliverSM(s.nextSeq(), dlr))
}
