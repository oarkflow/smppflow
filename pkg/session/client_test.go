package session_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/server"
	"github.com/oarkflow/smppflow/pkg/session"
)

const testTimeout = 2 * time.Second

// newServer starts a test SMPP server on 127.0.0.1:0 and blocks on Ready so
// tests never race the listener bind. Teardown closes the listener and waits
// for ListenAndServe to return.
func newServer(t *testing.T, cfg server.Config) *server.Server {
	t.Helper()
	cfg.Addr = "127.0.0.1:0"
	if cfg.SystemID == "" {
		cfg.SystemID = "smsc"
	}
	srv := server.New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(ctx) }()
	select {
	case <-srv.Ready():
	case <-time.After(testTimeout):
		cancel()
		t.Fatal("server did not bind in time")
	}
	t.Cleanup(func() {
		_ = srv.Close()
		cancel()
		select {
		case <-errc:
		case <-time.After(testTimeout):
			t.Error("ListenAndServe did not return after Close")
		}
	})
	return srv
}

// newClient wires a client to addr with the heartbeat disabled so tests only
// see the PDUs they provoke. It is closed via t.Cleanup.
func newClient(t *testing.T, addr string) *session.Client {
	t.Helper()
	cli := session.NewClient(session.Config{
		Addr: addr,
		Bind: protocol.BindRequest{
			Mode:             protocol.BindModeTransceiver,
			SystemID:         "esme",
			Password:         "pw",
			InterfaceVersion: protocol.Version34,
		},
		EnquireInterval: time.Hour,
		ResponseTimeout: testTimeout,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		_ = cli.Close(ctx)
	})
	return cli
}

func connectAndBind(t *testing.T, cli *session.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := cli.Bind(ctx); err != nil {
		t.Fatalf("Bind: %v", err)
	}
}

func testSubmitReq() protocol.SubmitSMRequest {
	return protocol.SubmitSMRequest{
		Source:       protocol.Address{TON: protocol.TONAlphanumeric, NPI: protocol.NPIUnknown, Addr: "BRAND"},
		Destination:  protocol.Address{TON: protocol.TONInternational, NPI: protocol.NPIISDN, Addr: "9779841000000"},
		ShortMessage: []byte("hello"),
	}
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestClientConnectBindState(t *testing.T) {
	srv := newServer(t, server.Config{})
	cli := newClient(t, srv.Addr())

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if got := cli.State(); got != lifecycle.ConnNew {
		t.Fatalf("initial state = %q, want %q", got, lifecycle.ConnNew)
	}
	if err := cli.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if got := cli.State(); got != lifecycle.ConnConnected {
		t.Fatalf("state after Connect = %q, want %q", got, lifecycle.ConnConnected)
	}
	if err := cli.Bind(ctx); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if got := cli.State(); got != lifecycle.ConnBound {
		t.Fatalf("state after Bind = %q, want %q", got, lifecycle.ConnBound)
	}
}

func TestClientSubmitSMRoundTrip(t *testing.T) {
	srv := newServer(t, server.Config{
		Submit: func(context.Context, protocol.SubmitSMRequest) (string, protocol.CommandStatus) {
			return "msg-42", protocol.ESME_ROK
		},
	})
	cli := newClient(t, srv.Addr())
	connectAndBind(t, cli)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	resp, err := cli.SubmitSM(ctx, testSubmitReq())
	if err != nil {
		t.Fatalf("SubmitSM: %v", err)
	}
	if resp.MessageID != "msg-42" {
		t.Fatalf("MessageID = %q, want msg-42", resp.MessageID)
	}
}

func TestClientQuerySMRoundTrip(t *testing.T) {
	srv := newServer(t, server.Config{
		Query: func(_ context.Context, req protocol.QuerySMRequest) (protocol.QuerySMRespData, protocol.CommandStatus) {
			return protocol.QuerySMRespData{
				MessageID:    req.MessageID,
				FinalDate:    "260101120000000+",
				MessageState: 7,
				ErrorCode:    3,
			}, protocol.ESME_ROK
		},
	})
	cli := newClient(t, srv.Addr())
	connectAndBind(t, cli)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	resp, err := cli.QuerySM(ctx, protocol.QuerySMRequest{MessageID: "msg-7", Source: protocol.Address{Addr: "14155550100"}})
	if err != nil {
		t.Fatalf("QuerySM: %v", err)
	}
	if resp.MessageID != "msg-7" || resp.MessageState != 7 || resp.ErrorCode != 3 || resp.FinalDate != "260101120000000+" {
		t.Fatalf("resp = %+v, want message_id=msg-7 state=7 err=3", resp)
	}
}

func TestClientRejectsUseBeforeConnectAndBind(t *testing.T) {
	srv := newServer(t, server.Config{})
	addr := srv.Addr()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	fresh := newClient(t, addr)
	if _, err := fresh.SubmitSM(ctx, testSubmitReq()); err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("SubmitSM before Connect: err = %v, want not connected", err)
	}
	if _, err := fresh.SendPDU(ctx, protocol.BuildEnquireLink(1)); err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("SendPDU before Connect: err = %v, want not connected", err)
	}

	// Connected but unbound: the server answers submit_sm with ESME_RINVBNDSTS
	// and SubmitSM surfaces that status as an error.
	connected := newClient(t, addr)
	if err := connected.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if _, err := connected.SubmitSM(ctx, testSubmitReq()); err == nil || !strings.Contains(err.Error(), "submit_sm failed") {
		t.Fatalf("SubmitSM before Bind: err = %v, want submit_sm failure", err)
	}

	// Session commands require a successful bind, so query_sm is rejected with
	// ESME_RINVBNDSTS before Bind and surfaces as an error.
	q, err := connected.QuerySM(ctx, protocol.QuerySMRequest{MessageID: "m1"})
	if err == nil {
		t.Fatalf("QuerySM before Bind: expected rejection, got %+v", q)
	}
	if !strings.Contains(err.Error(), "query_sm failed") {
		t.Fatalf("QuerySM before Bind: err = %v, want query_sm failure", err)
	}
}

func TestClientNextSeqMonotonic(t *testing.T) {
	cli := newClient(t, "127.0.0.1:0")
	prev := uint32(0)
	for i := 0; i < 1000; i++ {
		n := cli.NextSeq()
		if n == 0 {
			t.Fatalf("NextSeq returned 0 on call %d", i+1)
		}
		if n <= prev {
			t.Fatalf("NextSeq not monotonic on call %d: %d after %d", i+1, n, prev)
		}
		prev = n
	}
}

func TestClientCloseIsSafeAndSubsequentOpsFail(t *testing.T) {
	srv := newServer(t, server.Config{})
	cli := newClient(t, srv.Addr())
	connectAndBind(t, cli)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := cli.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := cli.State(); got != lifecycle.ConnClosed {
		t.Fatalf("state after Close = %q, want %q", got, lifecycle.ConnClosed)
	}
	// Close is idempotent: repeated calls are a no-op returning nil.
	if err := cli.Close(ctx); err != nil {
		t.Fatalf("repeated Close: %v, want nil", err)
	}
	if err := cli.Close(ctx); err != nil {
		t.Fatalf("third Close: %v, want nil", err)
	}

	if _, err := cli.SubmitSM(ctx, testSubmitReq()); err == nil {
		t.Fatal("SubmitSM after Close must return an error")
	}
}

func TestClientWindowSizeAndPending(t *testing.T) {
	if got := session.NewClient(session.Config{}).WindowSize(); got != 16 {
		t.Fatalf("default WindowSize = %d, want 16", got)
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()

	srv := newServer(t, server.Config{
		Submit: func(context.Context, protocol.SubmitSMRequest) (string, protocol.CommandStatus) {
			<-release
			return "slow-1", protocol.ESME_ROK
		},
	})
	cli := session.NewClient(session.Config{
		Addr: srv.Addr(),
		Bind: protocol.BindRequest{
			Mode:             protocol.BindModeTransceiver,
			SystemID:         "esme",
			Password:         "pw",
			InterfaceVersion: protocol.Version34,
		},
		WindowSize:      1,
		EnquireInterval: time.Hour,
		ResponseTimeout: testTimeout,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		_ = cli.Close(ctx)
	})
	connectAndBind(t, cli)

	if got := cli.WindowSize(); got != 1 {
		t.Fatalf("WindowSize = %d, want 1", got)
	}
	if got := cli.Pending(); got != 0 {
		t.Fatalf("Pending before any send = %d, want 0", got)
	}

	type result struct {
		resp protocol.SubmitSMRespData
		err  error
	}
	first := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		resp, err := cli.SubmitSM(ctx, testSubmitReq())
		first <- result{resp, err}
	}()

	// The blocked Submit handler keeps the first PDU in flight.
	waitFor(t, testTimeout, "submit_sm to be in flight", func() bool { return cli.Pending() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if _, err := cli.SubmitSM(ctx, testSubmitReq()); err == nil || !strings.Contains(err.Error(), "window full") {
		t.Fatalf("SubmitSM beyond window: err = %v, want window full", err)
	}

	unblock()
	select {
	case r := <-first:
		if r.err != nil {
			t.Fatalf("first SubmitSM: %v", r.err)
		}
		if r.resp.MessageID != "slow-1" {
			t.Fatalf("MessageID = %q, want slow-1", r.resp.MessageID)
		}
	case <-time.After(testTimeout):
		t.Fatal("first SubmitSM did not complete")
	}

	waitFor(t, testTimeout, "pending to drain", func() bool { return cli.Pending() == 0 })
}

func TestClientConnectToClosedPortFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot bind: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	cli := newClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := cli.Connect(ctx); err == nil {
		t.Fatal("Connect to an unused port must return an error")
	}
	if got := cli.State(); got != lifecycle.ConnFailed {
		t.Fatalf("state after failed Connect = %q, want %q", got, lifecycle.ConnFailed)
	}
}
