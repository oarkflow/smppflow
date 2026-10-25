package smppserver_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/security"
	"github.com/oarkflow/smppflow/pkg/server"
	"github.com/oarkflow/smppflow/pkg/session"
	"github.com/oarkflow/smppflow/pkg/smppserver"
)

func TestMemoryAccountsSaveDefaultsAndGet(t *testing.T) {
	m := smppserver.NewMemoryAccounts()
	if _, err := m.GetBySystemID(context.Background(), "missing"); err == nil {
		t.Fatal("Get on missing account must fail")
	}
	m.Save(smppserver.Account{SystemID: "a", TenantID: "t", Enabled: true})
	got, err := m.GetBySystemID(context.Background(), "a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.MaxBinds != 1 {
		t.Fatalf("MaxBinds=%d want 1", got.MaxBinds)
	}
	if got.TPS != 100 {
		t.Fatalf("TPS=%d want 100", got.TPS)
	}
	m.Save(smppserver.Account{SystemID: "a", TenantID: "t", Enabled: true, MaxBinds: 5, TPS: 25})
	got, err = m.GetBySystemID(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxBinds != 5 || got.TPS != 25 {
		t.Fatalf("explicit values must win: %+v", got)
	}
}

func TestRegistryAddRemoveList(t *testing.T) {
	r := smppserver.NewRegistry()
	s1 := smppserver.SessionInfo{ID: "s1", SystemID: "sys", TenantID: "t", Mode: protocol.BindModeTransceiver, BoundAt: time.Now()}
	s2 := smppserver.SessionInfo{ID: "s2", SystemID: "sys", TenantID: "t"}
	if err := r.Add(s1, 2); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := r.Add(s2, 2); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := r.Add(smppserver.SessionInfo{ID: "s3", SystemID: "sys"}, 2); err == nil {
		t.Fatal("third bind must exceed max=2")
	}
	if err := r.Add(smppserver.SessionInfo{ID: "s3", SystemID: "other"}, 2); err != nil {
		t.Fatalf("other system_id must not share the limit: %v", err)
	}
	list := r.List()
	if len(list) != 3 {
		t.Fatalf("list=%d want 3", len(list))
	}
	r.Remove("s1")
	list = r.List()
	if len(list) != 2 {
		t.Fatalf("after Remove list=%d want 2", len(list))
	}
	if err := r.Add(smppserver.SessionInfo{ID: "s4", SystemID: "sys"}, 2); err != nil {
		t.Fatalf("slot must free up after Remove: %v", err)
	}
	if err := r.Add(smppserver.SessionInfo{ID: "s5", SystemID: "unlimited"}, 0); err != nil {
		t.Fatalf("max=0 must be unlimited: %v", err)
	}
	if err := r.Add(smppserver.SessionInfo{ID: "s6", SystemID: "unlimited"}, 0); err != nil {
		t.Fatalf("max=0 must be unlimited: %v", err)
	}
}

type fakeAddr string

func (f fakeAddr) Network() string { return "tcp" }
func (f fakeAddr) String() string  { return string(f) }

type fakeConn struct {
	net.Conn
	remote fakeAddr
}

func (f fakeConn) RemoteAddr() net.Addr { return f.remote }

func TestRemoteIP(t *testing.T) {
	got := smppserver.RemoteIP(fakeConn{remote: fakeAddr("10.1.2.3:5678")})
	if got != "10.1.2.3" {
		t.Fatalf("RemoteIP=%q want 10.1.2.3", got)
	}
	got = smppserver.RemoteIP(fakeConn{remote: fakeAddr("pipe")})
	if got != "pipe" {
		t.Fatalf("RemoteIP=%q want pipe fallback", got)
	}
}

func startAuthedServer(t *testing.T, accounts smppserver.AccountStore) *smppserver.Server {
	t.Helper()
	srv := smppserver.New(smppserver.Config{
		Addr:     "127.0.0.1:0",
		SystemID: "smsc",
		Version:  protocol.Version34,
		Accounts: accounts,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(ctx) }()
	select {
	case <-srv.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not bind")
	}
	t.Cleanup(func() {
		_ = srv.Close()
		select {
		case <-errc:
		case <-time.After(2 * time.Second):
			t.Error("server did not stop")
		}
	})
	return srv
}

func bindClient(t *testing.T, addr string, req protocol.BindRequest) error {
	t.Helper()
	client := session.NewClient(session.Config{
		Addr: addr,
		Bind: req,
	})
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return client.Bind(ctx)
}

func TestAccountAuthAcceptsValidPassword(t *testing.T) {
	hash, err := security.HashPassword("good-pw")
	if err != nil {
		t.Fatal(err)
	}
	accounts := smppserver.NewMemoryAccounts()
	accounts.Save(smppserver.Account{SystemID: "c1", TenantID: "t", PasswordHash: hash, Enabled: true})
	srv := startAuthedServer(t, accounts)
	err = bindClient(t, srv.Addr(), protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "c1", Password: "good-pw", InterfaceVersion: protocol.Version34})
	if err != nil {
		t.Fatalf("bind with correct password must succeed: %v", err)
	}
}

func TestAccountAuthRejectsWrongPassword(t *testing.T) {
	hash, err := security.HashPassword("good-pw")
	if err != nil {
		t.Fatal(err)
	}
	accounts := smppserver.NewMemoryAccounts()
	accounts.Save(smppserver.Account{SystemID: "c1", TenantID: "t", PasswordHash: hash, Enabled: true})
	srv := startAuthedServer(t, accounts)
	err = bindClient(t, srv.Addr(), protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "c1", Password: "bad-pw", InterfaceVersion: protocol.Version34})
	if err == nil || !strings.Contains(err.Error(), protocol.ESME_RINVPASWD.String()) {
		t.Fatalf("err=%v want %s", err, protocol.ESME_RINVPASWD)
	}
}

func TestAccountAuthRejectsUnknownDisabledAndWrongMode(t *testing.T) {
	hash, err := security.HashPassword("good-pw")
	if err != nil {
		t.Fatal(err)
	}
	accounts := smppserver.NewMemoryAccounts()
	accounts.Save(smppserver.Account{SystemID: "ok", TenantID: "t", PasswordHash: hash, Enabled: true, Modes: []protocol.BindMode{protocol.BindModeTransmitter}})
	accounts.Save(smppserver.Account{SystemID: "off", TenantID: "t", PasswordHash: hash, Enabled: false})
	srv := startAuthedServer(t, accounts)

	err = bindClient(t, srv.Addr(), protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "ghost", Password: "good-pw", InterfaceVersion: protocol.Version34})
	if err == nil || !strings.Contains(err.Error(), protocol.ESME_RINVSYSID.String()) {
		t.Fatalf("unknown system_id: err=%v want %s", err, protocol.ESME_RINVSYSID)
	}

	err = bindClient(t, srv.Addr(), protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "off", Password: "good-pw", InterfaceVersion: protocol.Version34})
	if err == nil || !strings.Contains(err.Error(), protocol.ESME_RINVSYSID.String()) {
		t.Fatalf("disabled account: err=%v want %s", err, protocol.ESME_RINVSYSID)
	}

	err = bindClient(t, srv.Addr(), protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "ok", Password: "good-pw", InterfaceVersion: protocol.Version34})
	if err == nil || !strings.Contains(err.Error(), protocol.ESME_RBINDFAIL.String()) {
		t.Fatalf("mode mismatch: err=%v want %s", err, protocol.ESME_RBINDFAIL)
	}

	err = bindClient(t, srv.Addr(), protocol.BindRequest{Mode: protocol.BindModeTransmitter, SystemID: "ok", Password: "good-pw", InterfaceVersion: protocol.Version34})
	if err != nil {
		t.Fatalf("allowed mode must bind: %v", err)
	}
}

func TestAccountAuthAcceptsLegacySHA256Hash(t *testing.T) {
	sum := sha256.Sum256([]byte("legacy-pw"))
	accounts := smppserver.NewMemoryAccounts()
	accounts.Save(smppserver.Account{SystemID: "c1", TenantID: "t", PasswordHash: hex.EncodeToString(sum[:]), Enabled: true})
	srv := startAuthedServer(t, accounts)
	if err := bindClient(t, srv.Addr(), protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "c1", Password: "legacy-pw", InterfaceVersion: protocol.Version34}); err != nil {
		t.Fatalf("legacy hash must keep working: %v", err)
	}
}

func TestAuthWithoutAccountStoreFallsBackToInner(t *testing.T) {
	srv := smppserver.New(smppserver.Config{
		Addr:     "127.0.0.1:0",
		SystemID: "smsc",
		Version:  protocol.Version34,
		Inner: server.Config{
			Auth: func(req protocol.BindRequest) protocol.CommandStatus {
				if req.SystemID == "anyone" {
					return protocol.ESME_ROK
				}
				return protocol.ESME_RINVSYSID
			},
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(ctx) }()
	select {
	case <-srv.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not bind")
	}
	t.Cleanup(func() {
		_ = srv.Close()
		select {
		case <-errc:
		case <-time.After(2 * time.Second):
			t.Error("server did not stop")
		}
	})
	client := session.NewClient(session.Config{
		Addr: srv.Addr(),
		Bind: protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "anyone", InterfaceVersion: protocol.Version34},
	})
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ccancel()
	if err := client.Connect(cctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Bind(cctx); err != nil {
		t.Fatalf("bind without account store must fall back to inner auth: %v", err)
	}
	if client.State() != lifecycle.ConnBound {
		t.Fatalf("state=%v want bound", client.State())
	}
}
