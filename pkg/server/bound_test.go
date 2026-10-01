package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/codec"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/server"
)

// TestUnboundSessionsCannotRunSessionCommands is a regression test: query_sm,
// cancel_sm, replace_sm and the broadcast family used to be served before bind,
// letting an unauthenticated peer inspect and modify messages.
func TestUnboundSessionsCannotRunSessionCommands(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := server.New(server.Config{Addr: "127.0.0.1:0", SystemID: "smsc", Version: protocol.Version34})
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(ctx) }()
	select {
	case <-srv.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not bind in time")
	}
	defer func() { _ = srv.Close(); <-errc }()

	conn, err := net.DialTimeout("tcp", srv.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	cases := []struct {
		name    string
		req     protocol.PDU
		wantCmd protocol.CommandID
	}{
		{"query_sm", protocol.BuildQuerySM(1, protocol.QuerySMRequest{MessageID: "m1"}), protocol.QuerySMResp},
		{"cancel_sm", protocol.BuildCancelSM(2, protocol.CancelSMRequest{MessageID: "m1"}), protocol.CancelSMResp},
		{"replace_sm", protocol.BuildReplaceSM(3, protocol.ReplaceSMRequest{MessageID: "m1"}), protocol.ReplaceSMResp},
		{"submit_multi", protocol.BuildSubmitMulti(4, protocol.SubmitMultiRequest{}), protocol.SubmitMultiResp},
		{"query_broadcast_sm", protocol.BuildQueryBroadcastSM(5, protocol.QueryBroadcastSMRequest{MessageID: "m1"}), protocol.QueryBroadcastSMResp},
		{"cancel_broadcast_sm", protocol.BuildCancelBroadcastSM(6, protocol.CancelBroadcastSMRequest{MessageID: "m1"}), protocol.CancelBroadcastSMResp},
	}
	for _, c := range cases {
		if err := codec.WritePDU(conn, c.req); err != nil {
			t.Fatalf("%s: write: %v", c.name, err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		resp, err := codec.ReadPDU(conn)
		if err != nil {
			t.Fatalf("%s: read: %v", c.name, err)
		}
		if resp.CommandID != c.wantCmd {
			t.Errorf("%s: response command = %s, want %s", c.name, resp.CommandID, c.wantCmd)
		}
		if resp.CommandStatus != protocol.ESME_RINVBNDSTS {
			t.Errorf("%s: status = %s, want %s", c.name, resp.CommandStatus, protocol.ESME_RINVBNDSTS)
		}
	}
}

// TestUnboundSubmitAndDataStillRejected keeps the pre-existing guarantee that
// submit_sm and data_sm require bind.
func TestUnboundSubmitAndDataStillRejected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := server.New(server.Config{Addr: "127.0.0.1:0", SystemID: "smsc", Version: protocol.Version34})
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(ctx) }()
	select {
	case <-srv.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not bind in time")
	}
	defer func() { _ = srv.Close(); <-errc }()

	conn, err := net.DialTimeout("tcp", srv.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := codec.WritePDU(conn, protocol.BuildSubmitSM(1, protocol.SubmitSMRequest{})); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := codec.ReadPDU(conn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.CommandID != protocol.SubmitSMResp {
		t.Fatalf("command = %s, want submit_sm_resp", resp.CommandID)
	}
	if resp.CommandStatus != protocol.ESME_RINVBNDSTS {
		t.Fatalf("status = %s, want %s", resp.CommandStatus, protocol.ESME_RINVBNDSTS)
	}
}

// TestEnquireLinkAllowedBeforeBind: keepalive must work pre-bind so a client can
// hold a connection open while authenticating.
func TestEnquireLinkAllowedBeforeBind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := server.New(server.Config{Addr: "127.0.0.1:0", SystemID: "smsc", Version: protocol.Version34})
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(ctx) }()
	select {
	case <-srv.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not bind in time")
	}
	defer func() { _ = srv.Close(); <-errc }()

	conn, err := net.DialTimeout("tcp", srv.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := codec.WritePDU(conn, protocol.BuildEnquireLink(1)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := codec.ReadPDU(conn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.CommandID != protocol.EnquireLinkResp {
		t.Fatalf("command = %s, want enquire_link_resp", resp.CommandID)
	}
	if resp.CommandStatus != protocol.ESME_ROK {
		t.Fatalf("status = %s, want %s", resp.CommandStatus, protocol.ESME_ROK)
	}
}
