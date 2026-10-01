package session

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/smppflow/pkg/codec"
	"github.com/oarkflow/smppflow/pkg/events"
	"github.com/oarkflow/smppflow/pkg/hooks"
	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/protocol"
)

type Config struct {
	Addr             string
	TLS              *tls.Config
	Bind             protocol.BindRequest
	EnquireInterval  time.Duration
	ResponseTimeout  time.Duration
	WindowSize       int
	AutoReconnect    bool
	ReconnectBackoff time.Duration
	Bus              events.Bus
	Hooks            *hooks.Manager
}
type Client struct {
	cfg       Config
	mu        sync.Mutex
	conn      net.Conn
	closeOnce sync.Once
	state     lifecycle.ConnectionState
	seq       atomic.Uint32
	pendingMu sync.Mutex
	pending   map[uint32]chan protocol.PDU
	inbound   chan protocol.PDU
	closed    chan struct{}
}

// setConn stores the active connection. conn is guarded by mu because Connect,
// SendPDU, readLoop and Close all touch it from different goroutines.
func (c *Client) setConn(conn net.Conn) {
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
}

// currentConn returns the active connection, or nil once the client is closed.
func (c *Client) currentConn() net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

// markClosed closes the closed channel exactly once, regardless of how many
// goroutines observe the connection dropping.
func (c *Client) markClosed() { c.closeOnce.Do(func() { close(c.closed) }) }

func NewClient(cfg Config) *Client {
	if cfg.EnquireInterval == 0 {
		cfg.EnquireInterval = 30 * time.Second
	}
	if cfg.ResponseTimeout == 0 {
		cfg.ResponseTimeout = 10 * time.Second
	}
	if cfg.WindowSize == 0 {
		cfg.WindowSize = 16
	}
	if cfg.Bus == nil {
		cfg.Bus = events.NewMemoryBus(true)
	}
	if cfg.Hooks == nil {
		cfg.Hooks = hooks.New()
	}
	return &Client{cfg: cfg, state: lifecycle.ConnNew, pending: map[uint32]chan protocol.PDU{}, inbound: make(chan protocol.PDU, 128), closed: make(chan struct{})}
}
func (c *Client) State() lifecycle.ConnectionState     { c.mu.Lock(); defer c.mu.Unlock(); return c.state }
func (c *Client) WindowSize() int                      { return c.cfg.WindowSize }
func (c *Client) Pending() int                         { c.pendingMu.Lock(); defer c.pendingMu.Unlock(); return len(c.pending) }
func (c *Client) setState(s lifecycle.ConnectionState) { c.mu.Lock(); c.state = s; c.mu.Unlock() }
func (c *Client) NextSeq() uint32 {
	n := c.seq.Add(1)
	if n == 0 {
		n = c.seq.Add(1)
	}
	return n
}
func (c *Client) Connect(ctx context.Context) error {
	c.setState(lifecycle.ConnConnecting)
	_ = c.cfg.Hooks.Run(ctx, hooks.BeforeConnect, c)
	var conn net.Conn
	var err error
	d := net.Dialer{}
	if c.cfg.TLS != nil {
		conn, err = tls.DialWithDialer(&d, "tcp", c.cfg.Addr, c.cfg.TLS)
	} else {
		conn, err = d.DialContext(ctx, "tcp", c.cfg.Addr)
	}
	if err != nil {
		c.setState(lifecycle.ConnFailed)
		return err
	}
	c.setConn(conn)
	c.setState(lifecycle.ConnConnected)
	_ = c.cfg.Hooks.Run(ctx, hooks.AfterConnect, c)
	_ = c.cfg.Bus.Publish(ctx, events.Event{Topic: events.ConnectionOpened, Data: c.cfg.Addr})
	go c.readLoop()
	go c.heartbeat()
	return nil
}
func (c *Client) Bind(ctx context.Context) error {
	c.setState(lifecycle.ConnBinding)
	_ = c.cfg.Hooks.Run(ctx, hooks.BeforeBind, c.cfg.Bind)
	p := protocol.BuildBind(c.NextSeq(), c.cfg.Bind)
	resp, err := c.SendPDU(ctx, p)
	if err != nil {
		return err
	}
	if resp.CommandStatus != protocol.ESME_ROK {
		_ = c.cfg.Bus.Publish(ctx, events.Event{Topic: events.BindFailed, Data: resp.CommandStatus})
		return fmt.Errorf("bind failed: %s", resp.CommandStatus)
	}
	c.setState(lifecycle.ConnBound)
	_ = c.cfg.Hooks.Run(ctx, hooks.AfterBind, resp)
	_ = c.cfg.Bus.Publish(ctx, events.Event{Topic: events.BindSucceeded, Data: c.cfg.Bind.SystemID})
	return nil
}
func (c *Client) SendPDU(ctx context.Context, p protocol.PDU) (protocol.PDU, error) {
	conn := c.currentConn()
	if conn == nil {
		return protocol.PDU{}, errors.New("not connected")
	}
	select {
	case <-c.closed:
		return protocol.PDU{}, errors.New("client closed")
	default:
	}
	if p.SequenceNumber == 0 {
		p.SequenceNumber = c.NextSeq()
	}
	ch := make(chan protocol.PDU, 1)
	if !p.CommandID.IsResponse() {
		c.pendingMu.Lock()
		if len(c.pending) >= c.cfg.WindowSize {
			c.pendingMu.Unlock()
			return protocol.PDU{}, errors.New("smpp window full")
		}
		c.pending[p.SequenceNumber] = ch
		c.pendingMu.Unlock()
		defer func() { c.pendingMu.Lock(); delete(c.pending, p.SequenceNumber); c.pendingMu.Unlock() }()
	}
	_ = c.cfg.Hooks.Run(ctx, hooks.BeforePDUSend, p)
	c.mu.Lock()
	err := codec.WritePDU(conn, p)
	c.mu.Unlock()
	if err != nil {
		return protocol.PDU{}, err
	}
	_ = c.cfg.Bus.Publish(ctx, events.Event{Topic: events.PduSent, Data: p.CommandID.String()})
	_ = c.cfg.Hooks.Run(ctx, hooks.AfterPDUSend, p)
	if p.CommandID.IsResponse() {
		return protocol.PDU{}, nil
	}
	t := time.NewTimer(c.cfg.ResponseTimeout)
	defer t.Stop()
	select {
	case resp := <-ch:
		return resp, nil
	case <-t.C:
		return protocol.PDU{}, fmt.Errorf("timeout waiting for seq %d", p.SequenceNumber)
	case <-ctx.Done():
		return protocol.PDU{}, ctx.Err()
	}
}
func (c *Client) Inbound() <-chan protocol.PDU { return c.inbound }

// writePDU sends a PDU on the current connection under the write lock.
func (c *Client) writePDU(p protocol.PDU) error {
	conn := c.currentConn()
	if conn == nil {
		return errors.New("not connected")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return codec.WritePDU(conn, p)
}

func (c *Client) readLoop() {
	conn := c.currentConn()
	if conn == nil {
		return
	}
	for {
		p, err := codec.ReadPDU(conn)
		if err != nil {
			c.markClosed()
			_ = c.cfg.Bus.Publish(context.Background(), events.Event{Topic: events.ConnectionClosed, Data: err.Error()})
			return
		}
		_ = c.cfg.Bus.Publish(context.Background(), events.Event{Topic: events.PduReceived, Data: p.CommandID.String()})
		if p.CommandID == protocol.EnquireLink {
			_ = c.writePDU(protocol.BuildEnquireLinkResp(p.SequenceNumber))
			continue
		}
		if p.CommandID == protocol.Unbind {
			_ = c.writePDU(protocol.BuildUnbindResp(p.SequenceNumber))
			c.markClosed()
			return
		}
		if p.IsResponse() {
			c.pendingMu.Lock()
			ch := c.pending[p.SequenceNumber]
			c.pendingMu.Unlock()
			if ch != nil {
				ch <- p
				continue
			}
		}
		select {
		case c.inbound <- p:
		default:
		}
	}
}
func (c *Client) heartbeat() {
	ticker := time.NewTicker(c.cfg.EnquireInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), c.cfg.ResponseTimeout)
			_, _ = c.SendPDU(ctx, protocol.BuildEnquireLink(c.NextSeq()))
			cancel()
		case <-c.closed:
			return
		}
	}
}

// Close unbinds and tears the connection down. It is safe to call repeatedly and
// from multiple goroutines; only the first call does any work.
func (c *Client) Close(ctx context.Context) error {
	conn := c.currentConn()
	if conn == nil {
		return nil
	}
	select {
	case <-c.closed:
		return nil
	default:
	}
	c.setState(lifecycle.ConnUnbinding)
	// Best-effort unbind; the peer may already be gone.
	_, _ = c.SendPDU(ctx, protocol.BuildUnbind(c.NextSeq()))
	c.setState(lifecycle.ConnClosed)
	c.markClosed()
	return conn.Close()
}
func (c *Client) SubmitSM(ctx context.Context, req protocol.SubmitSMRequest) (protocol.SubmitSMRespData, error) {
	resp, err := c.SendPDU(ctx, protocol.BuildSubmitSM(c.NextSeq(), req))
	if err != nil {
		return protocol.SubmitSMRespData{}, err
	}
	if resp.CommandStatus != protocol.ESME_ROK {
		return protocol.SubmitSMRespData{}, fmt.Errorf("submit_sm failed: %s", resp.CommandStatus)
	}
	return protocol.ParseSubmitSMResp(resp)
}

func (c *Client) QuerySM(ctx context.Context, req protocol.QuerySMRequest) (protocol.QuerySMRespData, error) {
	resp, err := c.SendPDU(ctx, protocol.BuildQuerySM(c.NextSeq(), req))
	if err != nil {
		return protocol.QuerySMRespData{}, err
	}
	if resp.CommandStatus != protocol.ESME_ROK {
		return protocol.QuerySMRespData{}, fmt.Errorf("query_sm failed: %s", resp.CommandStatus)
	}
	return protocol.ParseQuerySMResp(resp)
}

func (c *Client) CancelSM(ctx context.Context, req protocol.CancelSMRequest) error {
	resp, err := c.SendPDU(ctx, protocol.BuildCancelSM(c.NextSeq(), req))
	if err != nil {
		return err
	}
	if resp.CommandStatus != protocol.ESME_ROK {
		return fmt.Errorf("cancel_sm failed: %s", resp.CommandStatus)
	}
	return nil
}

func (c *Client) ReplaceSM(ctx context.Context, req protocol.ReplaceSMRequest) error {
	resp, err := c.SendPDU(ctx, protocol.BuildReplaceSM(c.NextSeq(), req))
	if err != nil {
		return err
	}
	if resp.CommandStatus != protocol.ESME_ROK {
		return fmt.Errorf("replace_sm failed: %s", resp.CommandStatus)
	}
	return nil
}

func (c *Client) DataSM(ctx context.Context, req protocol.SubmitSMRequest) (protocol.SubmitSMRespData, error) {
	resp, err := c.SendPDU(ctx, protocol.BuildDataSM(c.NextSeq(), req))
	if err != nil {
		return protocol.SubmitSMRespData{}, err
	}
	if resp.CommandStatus != protocol.ESME_ROK {
		return protocol.SubmitSMRespData{}, fmt.Errorf("data_sm failed: %s", resp.CommandStatus)
	}
	return protocol.ParseDataSMResp(resp)
}

func (c *Client) SubmitMulti(ctx context.Context, req protocol.SubmitMultiRequest) (protocol.SubmitMultiRespData, error) {
	resp, err := c.SendPDU(ctx, protocol.BuildSubmitMulti(c.NextSeq(), req))
	if err != nil {
		return protocol.SubmitMultiRespData{}, err
	}
	if resp.CommandStatus != protocol.ESME_ROK {
		return protocol.SubmitMultiRespData{}, fmt.Errorf("submit_multi failed: %s", resp.CommandStatus)
	}
	return protocol.ParseSubmitMultiResp(resp)
}

func (c *Client) BroadcastSM(ctx context.Context, req protocol.BroadcastSMRequest) (protocol.SubmitSMRespData, error) {
	resp, err := c.SendPDU(ctx, protocol.BuildBroadcastSM(c.NextSeq(), req))
	if err != nil {
		return protocol.SubmitSMRespData{}, err
	}
	if resp.CommandStatus != protocol.ESME_ROK {
		return protocol.SubmitSMRespData{}, fmt.Errorf("broadcast_sm failed: %s", resp.CommandStatus)
	}
	return protocol.ParseBroadcastSMResp(resp)
}

func (c *Client) QueryBroadcastSM(ctx context.Context, req protocol.QueryBroadcastSMRequest) (protocol.QueryBroadcastSMRespData, error) {
	resp, err := c.SendPDU(ctx, protocol.BuildQueryBroadcastSM(c.NextSeq(), req))
	if err != nil {
		return protocol.QueryBroadcastSMRespData{}, err
	}
	if resp.CommandStatus != protocol.ESME_ROK {
		return protocol.QueryBroadcastSMRespData{}, fmt.Errorf("query_broadcast_sm failed: %s", resp.CommandStatus)
	}
	return protocol.ParseQueryBroadcastSMResp(resp)
}

func (c *Client) CancelBroadcastSM(ctx context.Context, req protocol.CancelBroadcastSMRequest) error {
	resp, err := c.SendPDU(ctx, protocol.BuildCancelBroadcastSM(c.NextSeq(), req))
	if err != nil {
		return err
	}
	if resp.CommandStatus != protocol.ESME_ROK {
		return fmt.Errorf("cancel_broadcast_sm failed: %s", resp.CommandStatus)
	}
	return nil
}

func (c *Client) DeliverSMResp(ctx context.Context, seq uint32, status protocol.CommandStatus) error {
	_, err := c.SendPDU(ctx, protocol.BuildDeliverSMResp(seq, status))
	return err
}
