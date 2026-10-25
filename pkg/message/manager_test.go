package message_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/server"
	"github.com/oarkflow/smppflow/pkg/session"
	"github.com/oarkflow/smppflow/pkg/storage"
)

func startSMSC(t *testing.T, autoDLR bool) string {
	t.Helper()
	srv := server.New(server.Config{
		Addr:     "127.0.0.1:0",
		AutoDLR:  autoDLR,
		DLRDelay: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(ctx) }()
	select {
	case <-srv.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("smsc did not bind")
	}
	t.Cleanup(func() {
		_ = srv.Close()
		select {
		case <-errc:
		case <-time.After(2 * time.Second):
			t.Error("smsc did not stop")
		}
	})
	return srv.Addr()
}

func boundClient(t *testing.T, addr string) *session.Client {
	t.Helper()
	client := session.NewClient(session.Config{
		Addr: addr,
		Bind: protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "esme", InterfaceVersion: protocol.Version34},
	})
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := client.Bind(ctx); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return client
}

func newManager(t *testing.T, client *session.Client) (*message.Manager, *storage.Memory) {
	t.Helper()
	store := storage.NewMemory()
	return message.NewManager(message.ManagerConfig{
		Client:     client,
		Store:      store,
		ProviderID: "prov",
	}), store
}

func TestNewManagerRequiresStore(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewManager without a Store must panic")
		}
	}()
	message.NewManager(message.ManagerConfig{})
}

func TestSubmitSingleSegment(t *testing.T) {
	client := boundClient(t, startSMSC(t, false))
	m, store := newManager(t, client)
	ctx := context.Background()

	res, err := m.Submit(ctx, message.SubmitRequest{
		TenantID: "tenant-a",
		From:     "BRAND",
		To:       "9779841000000",
		Text:     "Your OTP is 123456",
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if res.MessageID == "" || res.Parts != 1 || len(res.ProviderIDs) != 1 {
		t.Fatalf("result=%+v", res)
	}
	if res.State != lifecycle.MsgSubmitted {
		t.Fatalf("State=%q want submitted", res.State)
	}
	if res.ProviderIDs[0] == "" {
		t.Fatal("provider message id must be captured")
	}

	saved, err := store.GetMessage(ctx, res.MessageID)
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}
	if saved.State != lifecycle.MsgSubmitted || saved.ProviderID != "prov" {
		t.Fatalf("saved=%+v", saved)
	}
	if len(saved.Parts) != 1 || saved.Parts[0].State != lifecycle.MsgSubmitted {
		t.Fatalf("parts=%+v", saved.Parts)
	}
	if saved.Parts[0].ProviderMessageID != res.ProviderIDs[0] {
		t.Fatalf("part provider id=%q want %q", saved.Parts[0].ProviderMessageID, res.ProviderIDs[0])
	}
	if len(saved.Transitions) == 0 {
		t.Fatal("transitions must be recorded")
	}
}

func TestSubmitMultipart(t *testing.T) {
	client := boundClient(t, startSMSC(t, false))
	m, store := newManager(t, client)
	ctx := context.Background()

	res, err := m.Submit(ctx, message.SubmitRequest{
		TenantID: "tenant-a",
		From:     "BRAND",
		To:       "9779841000000",
		Text:     strings.Repeat("a", 200),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if res.Parts != 2 || len(res.ProviderIDs) != 2 {
		t.Fatalf("result=%+v want 2 parts", res)
	}
	saved, err := store.GetMessage(ctx, res.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved.Parts) != 2 {
		t.Fatalf("parts=%d want 2", len(saved.Parts))
	}
	for i, p := range saved.Parts {
		if p.Index != i+1 || p.Total != 2 {
			t.Fatalf("part %d: %+v", i, p)
		}
	}
}

func TestSubmitValidationErrors(t *testing.T) {
	client := boundClient(t, startSMSC(t, false))
	m, _ := newManager(t, client)
	ctx := context.Background()

	cases := []struct {
		name string
		req  message.SubmitRequest
	}{
		{"missing destination", message.SubmitRequest{TenantID: "t", From: "BRAND", Text: "hi"}},
		{"missing source", message.SubmitRequest{TenantID: "t", To: "9779", Text: "hi"}},
		{"scheduled", message.SubmitRequest{TenantID: "t", From: "A", To: "9779", Text: "hi", ScheduleAt: time.Now().Add(time.Hour)}},
		{"expired", message.SubmitRequest{TenantID: "t", From: "A", To: "9779", Text: "hi", ExpiresAt: time.Now().Add(-time.Hour)}},
	}
	for _, tc := range cases {
		if _, err := m.Submit(ctx, tc.req); err == nil {
			t.Fatalf("%s: expected error", tc.name)
		}
	}
}

func TestSubmitRequiresClient(t *testing.T) {
	m := message.NewManager(message.ManagerConfig{Store: storage.NewMemory()})
	if _, err := m.Submit(context.Background(), message.SubmitRequest{TenantID: "t", From: "A", To: "9779", Text: "hi"}); err == nil {
		t.Fatal("nil client must fail")
	}
}

func TestSubmitIdempotentReplayAndConflict(t *testing.T) {
	client := boundClient(t, startSMSC(t, false))
	m, _ := newManager(t, client)
	ctx := context.Background()

	req := message.SubmitRequest{
		TenantID:       "tenant-a",
		From:           "BRAND",
		To:             "9779841000000",
		Text:           "Your OTP is 123456",
		IdempotencyKey: "key-1",
	}
	first, err := m.Submit(ctx, req)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if first.Idempotent {
		t.Fatal("first submit must not be flagged idempotent")
	}

	replay, err := m.Submit(ctx, req)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Idempotent {
		t.Fatal("replay must be flagged idempotent")
	}
	if replay.MessageID != first.MessageID {
		t.Fatalf("replay MessageID=%q want %q", replay.MessageID, first.MessageID)
	}

	req.Text = "different payload"
	if _, err := m.Submit(ctx, req); err == nil {
		t.Fatal("same key with different payload must conflict")
	}
}

func TestSubmitTransportFailure(t *testing.T) {
	client := boundClient(t, startSMSC(t, false))
	m, store := newManager(t, client)
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, err := m.Submit(context.Background(), message.SubmitRequest{
		TenantID: "tenant-a",
		From:     "BRAND",
		To:       "9779841000000",
		Text:     "will fail",
	})
	if err == nil {
		t.Fatal("submit over a closed client must fail")
	}
	msgs, err := store.ListMessages(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages=%d want 1", len(msgs))
	}
	if msgs[0].State != lifecycle.MsgFailed {
		t.Fatalf("State=%q want failed", msgs[0].State)
	}
	if msgs[0].Parts[0].LastError == "" {
		t.Fatal("failed part must record the error")
	}
}

func TestStartInboundSavesDLR(t *testing.T) {
	client := boundClient(t, startSMSC(t, true))
	m, store := newManager(t, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.StartInbound(ctx)

	_, err := m.Submit(ctx, message.SubmitRequest{
		TenantID:           "tenant-a",
		From:               "BRAND",
		To:                 "9779841000000",
		Text:               "tracked",
		RegisteredDelivery: 1,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		dlrs, err := store.ListDLR(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(dlrs) == 1 {
			d := dlrs[0]
			if d.ProviderID != "prov" {
				t.Fatalf("ProviderID=%q want prov", d.ProviderID)
			}
			if d.State != lifecycle.MsgDelivered {
				t.Fatalf("State=%q want delivered", d.State)
			}
			if d.ProviderMessageID == "" {
				t.Fatal("DLR must carry the provider message id")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("DLR was never persisted")
}

func TestStartInboundWithoutClientIsNoop(t *testing.T) {
	m := message.NewManager(message.ManagerConfig{Store: storage.NewMemory()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		m.StartInbound(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StartInbound with nil client must return immediately")
	}
}
