package server

// Benchmark for PDU dispatch throughput without a live network connection.
//
// server.handle() reads/writes protocol.PDU frames over a net.Conn. Rather
// than binding a real TCP listener, this benchmark wires the server's handle
// loop to one end of an in-memory net.Pipe() and drives submit_sm traffic
// from the other end in-process. net.Pipe is a synchronous, unbuffered
// in-memory io.ReadWriter pair - no sockets, no OS network stack - so this
// isolates PDU parse+dispatch+build+encode cost from real I/O.

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/codec"
	"github.com/oarkflow/smppflow/pkg/protocol"
)

// BenchmarkServerHandleSubmitSM measures end-to-end PDU handling throughput:
// client writes a submit_sm, the server's handle loop parses it, invokes the
// configured Submit handler, builds+writes the response, and the client reads
// it back. This is the core per-message dispatch cost of the SMPP server.
func BenchmarkServerHandleSubmitSM(b *testing.B) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	s := New(Config{
		SystemID: "bench",
		Submit: func(ctx context.Context, req protocol.SubmitSMRequest) (string, protocol.CommandStatus) {
			return "1", protocol.ESME_ROK
		},
	})

	done := make(chan struct{})
	go func() {
		s.handle(context.Background(), serverConn)
		close(done)
	}()

	// Bind first, as submit_sm is rejected until bound.
	seq := uint32(1)
	bindReq := protocol.BuildBind(seq, protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "bench", Password: "pw"})
	if err := codec.WritePDU(clientConn, bindReq); err != nil {
		b.Fatal(err)
	}
	if _, err := codec.ReadPDU(clientConn); err != nil {
		b.Fatal(err)
	}

	req := protocol.SubmitSMRequest{
		Source:       protocol.Address{Addr: "14155550100"},
		Destination:  protocol.Address{Addr: "9779800000000"},
		ShortMessage: []byte("Hello, this is a benchmark SMS payload used to measure dispatch cost."),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq++
		p := protocol.BuildSubmitSM(seq, req)
		if err := codec.WritePDU(clientConn, p); err != nil {
			b.Fatal(err)
		}
		resp, err := codec.ReadPDU(clientConn)
		if err != nil {
			b.Fatal(err)
		}
		if resp.CommandStatus != protocol.ESME_ROK {
			b.Fatalf("unexpected status %s", resp.CommandStatus)
		}
	}
	b.StopTimer()

	unbind := protocol.BuildUnbind(seq + 1)
	_ = codec.WritePDU(clientConn, unbind)
	_, _ = codec.ReadPDU(clientConn) // drain unbind_resp so the server's write does not block
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		b.Fatal("server handle loop did not exit after unbind")
	}
}

// BenchmarkServerHandleSubmitSMPipelined measures throughput when the client
// pipelines several submit_sm PDUs before reading responses, approximating a
// high-window-size SMPP session under load.
func BenchmarkServerHandleSubmitSMPipelined(b *testing.B) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	s := New(Config{
		SystemID: "bench",
		Submit: func(ctx context.Context, req protocol.SubmitSMRequest) (string, protocol.CommandStatus) {
			return "1", protocol.ESME_ROK
		},
	})

	done := make(chan struct{})
	go func() {
		s.handle(context.Background(), serverConn)
		close(done)
	}()

	seq := uint32(1)
	bindReq := protocol.BuildBind(seq, protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "bench", Password: "pw"})
	if err := codec.WritePDU(clientConn, bindReq); err != nil {
		b.Fatal(err)
	}
	if _, err := codec.ReadPDU(clientConn); err != nil {
		b.Fatal(err)
	}

	req := protocol.SubmitSMRequest{
		Source:       protocol.Address{Addr: "14155550100"},
		Destination:  protocol.Address{Addr: "9779800000000"},
		ShortMessage: []byte("short bench payload"),
	}

	writeErrs := make(chan error, 1)
	go func() {
		for i := 0; i < b.N; i++ {
			seq++
			p := protocol.BuildSubmitSM(seq, req)
			if err := codec.WritePDU(clientConn, p); err != nil {
				writeErrs <- err
				return
			}
		}
		writeErrs <- nil
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := codec.ReadPDU(clientConn)
		if err != nil {
			b.Fatal(err)
		}
		if resp.CommandStatus != protocol.ESME_ROK {
			b.Fatalf("unexpected status %s", resp.CommandStatus)
		}
	}
	b.StopTimer()
	if err := <-writeErrs; err != nil {
		b.Fatal(err)
	}

	unbind := protocol.BuildUnbind(seq + 1)
	_ = codec.WritePDU(clientConn, unbind)
	_, _ = codec.ReadPDU(clientConn) // drain unbind_resp so the server's write does not block
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		b.Fatal("server handle loop did not exit after unbind")
	}
}
