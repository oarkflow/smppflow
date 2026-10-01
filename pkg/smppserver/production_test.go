package smppserver_test

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
	"github.com/oarkflow/smppflow/pkg/session"
	"github.com/oarkflow/smppflow/pkg/smppserver"
)

func TestProductionWrapperServesTLS(t *testing.T) {
	cert := certificate(t)
	srv := smppserver.New(smppserver.Config{
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
		TLS:  &tls.Config{InsecureSkipVerify: true}, // self-signed test certificate
		Bind: protocol.BindRequest{Mode: protocol.BindModeTransceiver, SystemID: "c", InterfaceVersion: protocol.Version34},
	})
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := client.Bind(ctx); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if client.State() != lifecycle.ConnBound {
		t.Fatalf("state=%v want %v", client.State(), lifecycle.ConnBound)
	}
}

func certificate(t *testing.T) tls.Certificate {
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
