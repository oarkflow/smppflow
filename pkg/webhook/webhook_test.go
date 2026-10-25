package webhook_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/webhook"
)

type fakeWebhookStore struct {
	mu       sync.Mutex
	attempts []message.WebhookEvent
}

func (f *fakeWebhookStore) SaveWebhookAttempt(ctx context.Context, ev message.WebhookEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts = append(f.attempts, ev)
	return nil
}

func (f *fakeWebhookStore) ListWebhookAttempts(ctx context.Context, f2 message.EventFilter) ([]message.WebhookEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]message.WebhookEvent(nil), f.attempts...), nil
}

func (f *fakeWebhookStore) last() message.WebhookEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.attempts) == 0 {
		return message.WebhookEvent{}
	}
	return f.attempts[len(f.attempts)-1]
}

func TestSignIsHMACSHA256(t *testing.T) {
	sig := webhook.Sign("secret", []byte(`{"a":1}`))
	if !strings.HasPrefix(sig, "sha256=") || len(sig) != len("sha256=")+64 {
		t.Fatalf("signature format: %q", sig)
	}
	if sig != webhook.Sign("secret", []byte(`{"a":1}`)) {
		t.Fatal("Sign must be deterministic")
	}
	if sig == webhook.Sign("other", []byte(`{"a":1}`)) {
		t.Fatal("different secrets must produce different signatures")
	}
	if sig == webhook.Sign("secret", []byte(`{"a":2}`)) {
		t.Fatal("different bodies must produce different signatures")
	}
}

func TestSendSuccessSignsAndStores(t *testing.T) {
	var gotBody []byte
	var gotSig, gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotSig = r.Header.Get("X-SMPPFlow-Signature")
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	store := &fakeWebhookStore{}
	d := webhook.Dispatcher{Client: srv.Client(), Secret: "top", Store: store}
	err := d.Send(context.Background(), srv.URL+"/hook", "tenant-a", "message.delivered", message.ID("m1"), map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if gotCT != "application/json" {
		t.Fatalf("content-type=%q", gotCT)
	}
	if gotSig != webhook.Sign("top", gotBody) {
		t.Fatalf("signature=%q does not match body %q", gotSig, gotBody)
	}
	if !strings.Contains(string(gotBody), `"k":"v"`) {
		t.Fatalf("body=%q", gotBody)
	}
	ev := store.last()
	if ev.DeliveredAt.IsZero() || ev.LastError != "" {
		t.Fatalf("event=%+v want delivered without error", ev)
	}
	if ev.Kind != "message.delivered" || ev.TenantID != "tenant-a" || ev.MessageID != "m1" {
		t.Fatalf("event=%+v", ev)
	}
}

func TestSendWithoutSecretOmitsSignature(t *testing.T) {
	gotSig := "unset"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSig = r.Header.Get("X-SMPPFlow-Signature")
	}))
	defer srv.Close()
	d := webhook.Dispatcher{Client: srv.Client()}
	if err := d.Send(context.Background(), srv.URL, "t", "k", "m", "payload"); err != nil {
		t.Fatal(err)
	}
	if gotSig != "" {
		t.Fatalf("signature header must be omitted without a secret, got %q", gotSig)
	}
}

func TestSendHTTPErrorStoresAttempt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	store := &fakeWebhookStore{}
	d := webhook.Dispatcher{Client: srv.Client(), Store: store}
	err := d.Send(context.Background(), srv.URL, "t", "k", "m", "payload")
	if err == nil || !strings.Contains(err.Error(), "502") {
		t.Fatalf("err=%v want 502 failure", err)
	}
	ev := store.last()
	if ev.LastError == "" || !ev.DeliveredAt.IsZero() {
		t.Fatalf("event=%+v want failed attempt", ev)
	}
}

func TestSendNetworkErrorStoresAttempt(t *testing.T) {
	store := &fakeWebhookStore{}
	d := webhook.Dispatcher{Client: &http.Client{Timeout: 100 * time.Millisecond}, Store: store}
	err := d.Send(context.Background(), "http://127.0.0.1:1", "t", "k", "m", "payload")
	if err == nil {
		t.Fatal("connection failure must return an error")
	}
	if store.last().LastError == "" {
		t.Fatal("failed attempt must be recorded")
	}
}

func TestSendRequiresURL(t *testing.T) {
	d := webhook.Dispatcher{}
	if err := d.Send(context.Background(), "", "t", "k", "m", "payload"); err == nil {
		t.Fatal("empty URL must fail")
	}
}

func TestSendWithRetryEventuallyDelivers(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := webhook.Dispatcher{Client: srv.Client()}
	res := d.SendWithRetry(context.Background(), webhook.RetryConfig{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}, srv.URL, "t", "k", "m", "payload")
	if !res.Delivered {
		t.Fatalf("result=%+v want delivered", res)
	}
	if res.Attempts != 3 {
		t.Fatalf("attempts=%d want 3", res.Attempts)
	}
}

func TestSendWithRetryExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	d := webhook.Dispatcher{Client: srv.Client()}
	res := d.SendWithRetry(context.Background(), webhook.RetryConfig{MaxAttempts: 2, InitialDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}, srv.URL, "t", "k", "m", "payload")
	if res.Delivered {
		t.Fatalf("result=%+v want failure", res)
	}
	if res.Attempts != 2 || res.LastError == "" {
		t.Fatalf("result=%+v", res)
	}
}

func TestSendWithRetryDefaultsAndContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	d := webhook.Dispatcher{Client: srv.Client()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res := d.SendWithRetry(ctx, webhook.RetryConfig{MaxAttempts: 5, InitialDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}, srv.URL, "t", "k", "m", "payload")
	if res.Delivered {
		t.Fatal("cancelled context must not deliver")
	}
	if res.LastError != context.Canceled.Error() {
		t.Fatalf("LastError=%q want context cancellation", res.LastError)
	}
	if res.Attempts != 1 {
		t.Fatalf("attempts=%d want 1 (stop after ctx cancel)", res.Attempts)
	}
}
