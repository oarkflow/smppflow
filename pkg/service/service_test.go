package service_test

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/account"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/provider"
	"github.com/oarkflow/smppflow/pkg/queue"
	"github.com/oarkflow/smppflow/pkg/routing"
	"github.com/oarkflow/smppflow/pkg/service"
	"github.com/oarkflow/smppflow/pkg/session"
	"github.com/oarkflow/smppflow/pkg/storage"
)

const waitTimeout = 3 * time.Second

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

// assertNoMessageBefore polls the store until `until` and fails if any message
// shows up early. Checking stops at `until` so a slow machine cannot turn an
// early-run job into a false pass/fail.
func assertNoMessageBefore(t *testing.T, ctx context.Context, store message.Store, until time.Time) {
	t.Helper()
	for time.Now().Before(until) {
		msgs, err := store.ListMessages(ctx)
		if err != nil {
			t.Fatalf("ListMessages: %v", err)
		}
		if len(msgs) != 0 {
			t.Fatalf("message processed before it was due: %+v", msgs[0])
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// unconnectedFactory hands the message manager a real *session.Client that is
// never connected. service.ClientFactory returns a concrete type, so the send
// path cannot be faked without a live SMPP connection; SubmitSM fails inside
// message.Manager after SaveMessage has already recorded the message. The
// orchestration tests therefore assert job processing and message recording,
// not provider delivery.
func unconnectedFactory(_ context.Context, p provider.Profile) (*session.Client, error) {
	return session.NewClient(session.Config{
		Addr:            p.Addr,
		Bind:            p.BindRequest(),
		WindowSize:      p.WindowSize,
		EnquireInterval: p.EnquireInterval,
		ResponseTimeout: p.ResponseTimeout,
	}), nil
}

// newRoutedService wires a router whose single rule routes tenant "t1" with
// prefix "977" to provider "prov-1".
func newRoutedService(t *testing.T, store message.Store) (*service.Service, *queue.Memory) {
	t.Helper()
	router := routing.New()
	if err := router.RegisterProvider(provider.Generic34("prov-1", "127.0.0.1:1", "sys", "pw")); err != nil {
		t.Fatalf("RegisterProvider: %v", err)
	}
	if err := router.RegisterRule(routing.Rule{ID: "np", TenantID: "t1", Prefix: "977", ProviderID: "prov-1", Priority: 1}); err != nil {
		t.Fatalf("RegisterRule: %v", err)
	}
	// A huge retry delay keeps a single processing attempt observable instead
	// of racing follow-up attempts into the store.
	svc := service.New(service.Config{
		Router:        router,
		Store:         store,
		ClientFactory: unconnectedFactory,
		RetryDelay:    time.Hour,
	})
	qm, ok := svc.Queue().(*queue.Memory)
	if !ok {
		t.Fatalf("Queue() is %T, want *queue.Memory", svc.Queue())
	}
	return svc, qm
}

func routedRequest() message.SubmitRequest {
	return message.SubmitRequest{TenantID: "t1", From: "BRAND", To: "9779841000000", Text: "hello"}
}

func listMessages(t *testing.T, ctx context.Context, store message.Store) []*message.Message {
	t.Helper()
	msgs, err := store.ListMessages(ctx)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	return msgs
}

func TestNewFillsDefaults(t *testing.T) {
	svc := service.New(service.Config{})
	t.Cleanup(func() { _ = svc.Close(context.Background()) })

	if svc.Router() == nil {
		t.Fatal("Router default is nil")
	}
	if svc.Store() == nil {
		t.Fatal("Store default is nil")
	}
	if svc.Queue() == nil {
		t.Fatal("Queue default is nil")
	}

	// Bus, Hooks and AccountStore have no accessors; inspect the unexported
	// Config via reflection so a missing default fails here instead of deep
	// inside Send or authentication.
	cfg := reflect.ValueOf(svc).Elem().FieldByName("cfg")
	for _, name := range []string{"Bus", "Hooks", "AccountStore"} {
		f := cfg.FieldByName(name)
		if !f.IsValid() {
			t.Fatalf("service.Config has no field %s", name)
		}
		if f.Kind() == reflect.Interface || f.Kind() == reflect.Pointer {
			if f.IsNil() {
				t.Fatalf("%s default is nil", name)
			}
		}
	}
}

func TestEnqueueProcessesJobAndRecordsMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	store := storage.NewMemory()
	svc, qm := newRoutedService(t, store)
	t.Cleanup(func() { _ = svc.Close(ctx) })

	if err := svc.Enqueue(ctx, routedRequest(), 0, time.Time{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	svc.StartWorkers(ctx, 1)

	waitFor(t, waitTimeout, "message to be recorded", func() bool {
		return len(listMessages(t, ctx, store)) > 0
	})
	msgs := listMessages(t, ctx, store)
	if len(msgs) != 1 {
		t.Fatalf("recorded %d messages, want 1", len(msgs))
	}
	m := msgs[0]
	if m.TenantID != "t1" || m.From.Addr != "BRAND" || m.To.Addr != "9779841000000" || m.Text != "hello" {
		t.Fatalf("message = %+v, want the enqueued request", m)
	}
	if m.ProviderID != "prov-1" {
		t.Fatalf("ProviderID = %q, want prov-1 from the routing rule", m.ProviderID)
	}

	// The worker reserved the job, ran the pipeline and requeued it for a
	// later retry (the delay is one hour in this setup).
	waitFor(t, waitTimeout, "job to settle after its attempt", func() bool {
		jobs := qm.Snapshot()
		return len(jobs) == 1 && jobs[0].Attempts == 1
	})
}

func TestEnqueueRunAtIsRespected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	store := storage.NewMemory()
	svc, _ := newRoutedService(t, store)
	t.Cleanup(func() { _ = svc.Close(ctx) })

	runAt := time.Now().Add(400 * time.Millisecond)
	if err := svc.Enqueue(ctx, routedRequest(), 0, runAt); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	svc.StartWorkers(ctx, 1)

	assertNoMessageBefore(t, ctx, store, runAt)

	waitFor(t, waitTimeout, "scheduled job to be processed", func() bool {
		return len(listMessages(t, ctx, store)) > 0
	})
}

func TestRegisterUserAndAuthenticateAPIKey(t *testing.T) {
	ctx := context.Background()
	st := account.NewMemory()
	svc := service.New(service.Config{AccountStore: st})

	user := account.User{ID: "user-1", TenantID: "tenant-1", Username: "alice"}
	if err := svc.RegisterUser(ctx, user); err != nil {
		t.Fatalf("RegisterUser: %v", err)
	}

	key, secret, err := account.NewAPIKey("tenant-1", "user-1", "primary")
	if err != nil {
		t.Fatalf("NewAPIKey: %v", err)
	}
	if err := st.SaveAPIKey(ctx, key); err != nil {
		t.Fatalf("SaveAPIKey: %v", err)
	}

	gotUser, gotKey, err := svc.AuthenticateAPIKey(ctx, secret)
	if err != nil {
		t.Fatalf("AuthenticateAPIKey: %v", err)
	}
	if gotUser.ID != "user-1" || gotUser.TenantID != "tenant-1" {
		t.Fatalf("user = %+v, want user-1/tenant-1", gotUser)
	}
	if gotKey.ID != key.ID {
		t.Fatalf("key = %+v, want %s", gotKey, key.ID)
	}

	if _, _, err := svc.AuthenticateAPIKey(ctx, "sk_invalid_secret"); !errors.Is(err, account.ErrInvalidCredential) {
		t.Fatalf("invalid key err = %v, want ErrInvalidCredential", err)
	}
	if _, _, err := svc.AuthenticateAPIKey(ctx, ""); !errors.Is(err, account.ErrInvalidCredential) {
		t.Fatalf("empty key err = %v, want ErrInvalidCredential", err)
	}
}

func TestCloseTwiceAndNoProcessingAfterClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	store := storage.NewMemory()
	svc, _ := newRoutedService(t, store)
	t.Cleanup(func() { _ = svc.Close(ctx) })

	if err := svc.Enqueue(ctx, routedRequest(), 0, time.Time{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := svc.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := svc.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if err := svc.Enqueue(ctx, routedRequest(), 0, time.Time{}); err == nil {
		t.Fatal("Enqueue after Close must fail")
	}

	// Close must also stop the worker goroutines, not just the queue.
	before := runtime.NumGoroutine()
	svc.StartWorkers(ctx, 4)
	waitFor(t, 2*time.Second, "workers start", func() bool { return runtime.NumGoroutine() > before+2 })
	if err := svc.Close(ctx); err != nil {
		t.Fatalf("Close with running workers: %v", err)
	}
	waitFor(t, 2*time.Second, "workers exit after Close", func() bool { return runtime.NumGoroutine() <= before+2 })

	// The queue is shut down, so workers started now can never reserve the
	// already-queued job and nothing may be processed.
	svc.StartWorkers(ctx, 1)
	assertNoMessageBefore(t, ctx, store, time.Now().Add(300*time.Millisecond))
}

func TestSendUnroutableReturnsError(t *testing.T) {
	cases := []struct {
		name   string
		router func(*routing.Router)
		req    message.SubmitRequest
	}{
		{
			name:   "empty router",
			router: func(*routing.Router) {},
			req:    routedRequest(),
		},
		{
			name: "provider without rule",
			router: func(r *routing.Router) {
				_ = r.RegisterProvider(provider.Generic34("p1", "addr", "sys", "pw"))
			},
			req: routedRequest(),
		},
		{
			name: "rule for another tenant",
			router: func(r *routing.Router) {
				_ = r.RegisterProvider(provider.Generic34("p1", "addr", "sys", "pw"))
				_ = r.RegisterRule(routing.Rule{ID: "r1", TenantID: "t2", Prefix: "977", ProviderID: "p1", Priority: 1})
			},
			req: routedRequest(),
		},
		{
			name: "disabled rule",
			router: func(r *routing.Router) {
				_ = r.RegisterProvider(provider.Generic34("p1", "addr", "sys", "pw"))
				_ = r.RegisterRule(routing.Rule{ID: "r1", TenantID: "t1", Prefix: "977", ProviderID: "p1", Priority: 1, Disabled: true})
			},
			req: routedRequest(),
		},
		{
			name: "destination outside the rule prefix",
			router: func(r *routing.Router) {
				_ = r.RegisterProvider(provider.Generic34("p1", "addr", "sys", "pw"))
				_ = r.RegisterRule(routing.Rule{ID: "r1", TenantID: "t1", Prefix: "1", ProviderID: "p1", Priority: 1})
			},
			req: routedRequest(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			router := routing.New()
			tc.router(router)
			svc := service.New(service.Config{Router: router, ClientFactory: unconnectedFactory})

			ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
			defer cancel()
			res, err := svc.Send(ctx, tc.req)
			if err == nil {
				t.Fatalf("Send returned no error, result %+v", res)
			}
		})
	}
}
