package server_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/server"
)

// TestServerLifecycleConcurrentAccess exercises Addr, Close and Ready while the
// listener is being established, so the race detector can observe the guard on
// the listener field.
func TestServerLifecycleConcurrentAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := server.New(server.Config{Addr: "127.0.0.1:0", SystemID: "smsc", Version: protocol.Version34})

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = srv.Addr()
				select {
				case <-srv.Ready():
				default:
				}
			}
		}()
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(ctx) }()

	select {
	case <-srv.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not bind in time")
	}
	if got := srv.Addr(); got == "" {
		t.Fatal("Addr returned empty string after bind")
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()
	select {
	case <-errc:
	case <-time.After(2 * time.Second):
		t.Fatal("ListenAndServe did not return after Close")
	}
	// Close is idempotent.
	if err := srv.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestServerReadyOnBindFailure(t *testing.T) {
	// Bind a listener ourselves so the server cannot bind the same address.
	other, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot bind: %v", err)
	}
	defer other.Close()
	bad := server.New(server.Config{Addr: other.Addr().String(), SystemID: "smsc"})
	done := make(chan struct{})
	go func() { _ = bad.ListenAndServe(context.Background()); close(done) }()
	select {
	case <-bad.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("Ready was not closed on bind failure")
	}
	<-done
}
