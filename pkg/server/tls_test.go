package server_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/server"
	"github.com/oarkflow/smppflow/pkg/session"
)

func TestTLSSMPPListenerRoundTrip(t *testing.T) {
	cert := testCertificate(t)
	srv := server.New(server.Config{
		Addr:     "127.0.0.1:0",
		TLS:      &tls.Config{Certificates: []tls.Certificate{cert}},
		SystemID: "smsc",
		Version:  protocol.Version34,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe(ctx) }()
	select {
	case <-srv.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("TLS server did not bind in time")
	}
	t.Cleanup(func() {
		_ = srv.Close()
		select {
		case <-errc:
		case <-time.After(2 * time.Second):
			t.Error("TLS server did not stop")
		}
	})

	client := session.NewClient(session.Config{
		Addr: srv.Addr(),
		TLS:  &tls.Config{InsecureSkipVerify: true}, // test certificate is self-signed
		Bind: protocol.BindRequest{
			Mode:             protocol.BindModeTransceiver,
			SystemID:         "client",
			Password:         "password",
			InterfaceVersion: protocol.Version34,
		},
		ResponseTimeout: time.Second,
	})
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("TLS Connect: %v", err)
	}
	if err := client.Bind(ctx); err != nil {
		t.Fatalf("TLS Bind: %v", err)
	}
	if client.State() != lifecycle.ConnBound {
		t.Fatalf("state=%v want bound", client.State())
	}
}

func testCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}
