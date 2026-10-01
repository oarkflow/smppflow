package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpapi "github.com/oarkflow/smppflow/pkg/api"
	"github.com/oarkflow/smppflow/pkg/manager"
	"github.com/oarkflow/smppflow/pkg/rbac"
	"github.com/oarkflow/smppflow/pkg/security"
	"github.com/oarkflow/smppflow/pkg/service"
	"github.com/oarkflow/smppflow/pkg/storage"
)

func newTestServer(t *testing.T) *httpapi.Server {
	t.Helper()
	st := storage.NewMemory()
	svc := service.New(service.Config{Store: st})
	s := httpapi.New(svc, st)
	s.Ops = manager.NewOperations(manager.NewStore(), st, nil, nil, nil)
	return s
}

func newAuthedServer(t *testing.T) (*httpapi.Server, *rbac.KeyStore, string) {
	t.Helper()
	s := newTestServer(t)
	keys := rbac.NewKeyStore()
	_, secret, err := keys.Create("tenant-a", "test", []string{"*"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	policy := rbac.NewPolicy()
	s.Auth = &httpapi.AuthConfig{
		Keys:   httpapi.KeyStoreAuth(keys),
		Policy: httpapi.PolicyAuthorize(policy),
	}
	s.Keys = keys
	return s, keys, secret
}

func do(t *testing.T, h http.Handler, method, path, secret string, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *strings.Reader
	if body == "" {
		rdr = strings.NewReader("")
	} else {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rdr)
	if secret != "" {
		req.Header.Set("Authorization", "Bearer "+secret)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealthzIsPublic(t *testing.T) {
	s, _, _ := newAuthedServer(t)
	rec := do(t, s.Handler(), http.MethodGet, "/healthz", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status=%d want 200", rec.Code)
	}
}

func TestMissingKeyIsRejected(t *testing.T) {
	s, _, _ := newAuthedServer(t)
	rec := do(t, s.Handler(), http.MethodGet, "/v1/messages", "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", rec.Code)
	}
	if rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatal("expected WWW-Authenticate challenge")
	}
}

func TestInvalidKeyIsRejected(t *testing.T) {
	s, _, _ := newAuthedServer(t)
	rec := do(t, s.Handler(), http.MethodGet, "/v1/messages", "sk_not_a_real_key", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", rec.Code)
	}
}

func TestXAPIKeyHeaderAccepted(t *testing.T) {
	s, _, secret := newAuthedServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/messages", strings.NewReader(""))
	req.Header.Set("X-API-Key", secret)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
}

func TestDisabledKeyIsRejected(t *testing.T) {
	s, keys, secret := newAuthedServer(t)
	var id string
	for _, k := range keys.List("") {
		id = k.ID
	}
	if err := keys.Disable(id); err != nil {
		t.Fatal(err)
	}
	rec := do(t, s.Handler(), http.MethodGet, "/v1/messages", secret, "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", rec.Code)
	}
}

func TestExpiredKeyIsRejected(t *testing.T) {
	s := newTestServer(t)
	keys := rbac.NewKeyStore()
	_, secret, err := keys.Create("tenant-a", "short", nil, time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	s.Auth = &httpapi.AuthConfig{Keys: httpapi.KeyStoreAuth(keys)}
	time.Sleep(2 * time.Millisecond)
	rec := do(t, s.Handler(), http.MethodGet, "/v1/messages", secret, "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", rec.Code)
	}
}

func TestScopeEnforcement(t *testing.T) {
	s := newTestServer(t)
	keys := rbac.NewKeyStore()
	_, secret, err := keys.Create("tenant-a", "readonly", []string{"message:read"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.Auth = &httpapi.AuthConfig{
		Keys:   httpapi.KeyStoreAuth(keys),
		Policy: httpapi.PolicyAuthorize(rbac.NewPolicy()),
	}
	if rec := do(t, s.Handler(), http.MethodGet, "/v1/messages", secret, ""); rec.Code != http.StatusOK {
		t.Fatalf("read allowed scope: status=%d want 200", rec.Code)
	}
	rec := do(t, s.Handler(), http.MethodPost, "/v1/messages/send", secret, `{}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("write denied scope: status=%d want 403 body=%s", rec.Code, rec.Body.String())
	}
}

func TestActorIDHeaderIsOverwritten(t *testing.T) {
	s := newTestServer(t)
	keys := rbac.NewKeyStore()
	k, secret, err := keys.Create("tenant-a", "actor", []string{"*"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.Auth = &httpapi.AuthConfig{Keys: httpapi.KeyStoreAuth(keys)}
	req := httptest.NewRequest(http.MethodGet, "/v1/manager/audit", strings.NewReader(""))
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("X-Actor-ID", "spoofed-actor")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	// The handler reads X-Actor-ID from the request; middleware must have
	// replaced the spoofed value with the authenticated key ID.
	if got := req.Header.Get("X-Actor-ID"); got != k.ID {
		t.Fatalf("X-Actor-ID=%q want %q (spoofed value must not survive)", got, k.ID)
	}
}

func TestTenantIsolationViaQuery(t *testing.T) {
	s := newTestServer(t)
	keys := rbac.NewKeyStore()
	_, secret, err := keys.Create("tenant-a", "scoped", []string{"*"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.Auth = &httpapi.AuthConfig{Keys: httpapi.KeyStoreAuth(keys)}
	// Middleware fills tenant_id from the principal when absent.
	req := httptest.NewRequest(http.MethodGet, "/v1/manager/audit", strings.NewReader(""))
	req.Header.Set("Authorization", "Bearer "+secret)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if req.URL.Query().Get("tenant_id") != "tenant-a" {
		t.Fatalf("tenant_id=%q want tenant-a", req.URL.Query().Get("tenant_id"))
	}
}

func TestIPPolicyDenies(t *testing.T) {
	s := newTestServer(t)
	keys := rbac.NewKeyStore()
	_, secret, err := keys.Create("tenant-a", "ip", []string{"*"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	ip := &security.IPPolicy{DenyCIDR: []string{"127.0.0.1/32"}}
	if err := ip.Compile(); err != nil {
		t.Fatal(err)
	}
	s.Auth = &httpapi.AuthConfig{Keys: httpapi.KeyStoreAuth(keys), IPPolicy: ip}
	req := httptest.NewRequest(http.MethodGet, "/v1/messages", strings.NewReader(""))
	req.RemoteAddr = "127.0.0.1:5555"
	req.Header.Set("Authorization", "Bearer "+secret)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d want 403", rec.Code)
	}
	// A different source address is allowed.
	req = httptest.NewRequest(http.MethodGet, "/v1/messages", strings.NewReader(""))
	req.RemoteAddr = "127.0.0.2:5555"
	req.Header.Set("Authorization", "Bearer "+secret)
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("allowed source status=%d want 200", rec.Code)
	}
}

func TestRateLimit(t *testing.T) {
	s := newTestServer(t)
	keys := rbac.NewKeyStore()
	_, secret, err := keys.Create("tenant-a", "rl", []string{"*"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.Auth = &httpapi.AuthConfig{Keys: httpapi.KeyStoreAuth(keys), MaxRequestsPerMinute: 2}
	h := s.Handler()
	var codes []int
	for i := 0; i < 4; i++ {
		codes = append(codes, do(t, h, http.MethodGet, "/v1/messages", secret, "").Code)
	}
	ok, limited := 0, 0
	for _, c := range codes {
		switch c {
		case http.StatusOK:
			ok++
		case http.StatusTooManyRequests:
			limited++
		}
	}
	if ok != 2 || limited != 2 {
		t.Fatalf("codes=%v want 2 OK then 2 throttled", codes)
	}
}

func TestUnauthenticatedModeAllowsRequests(t *testing.T) {
	s := newTestServer(t)
	if rec := do(t, s.Handler(), http.MethodGet, "/v1/messages", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 with auth disabled", rec.Code)
	}
}

func TestAPIKeyLifecycle(t *testing.T) {
	s, _, secret := newAuthedServer(t)
	h := s.Handler()
	// Create a key through the API.
	body := `{"tenant_id":"tenant-a","name":"ci","scopes":["message:read"],"ttl":"1h"}`
	rec := do(t, h, http.MethodPost, "/v1/manager/api-keys", secret, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		ID     string `json:"id"`
		Secret string `json:"secret"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Secret == "" {
		t.Fatal("create must return the raw secret exactly once")
	}
	// List must never expose the hash or the secret.
	rec = do(t, h, http.MethodGet, "/v1/manager/api-keys?tenant_id=tenant-a", secret, "")
	var listed []rbac.APIKey
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	for _, k := range listed {
		if k.Hash != "" {
			t.Fatal("list must not expose key hashes")
		}
	}
	// The new key authenticates.
	if rec := do(t, h, http.MethodGet, "/v1/messages", created.Secret, ""); rec.Code != http.StatusOK {
		t.Fatalf("new key status=%d", rec.Code)
	}
	// Disable it.
	rec = do(t, h, http.MethodDelete, "/v1/manager/api-keys?id="+created.ID, secret, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("disable status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := do(t, h, http.MethodGet, "/v1/messages", created.Secret, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("disabled key status=%d want 401", rec.Code)
	}
}

func TestResolveActionMapping(t *testing.T) {
	cases := []struct {
		method, path, wantRes, wantAct string
	}{
		{http.MethodGet, "/v1/messages", "message", "read"},
		{http.MethodPost, "/v1/messages/send", "message", "write"},
		{http.MethodGet, "/v1/reports/summary", "report", "read"},
		{http.MethodGet, "/v1/reports/custom/x", "report", "read"},
		{http.MethodPost, "/v1/manager/queue/replay-dlq", "queue", "write"},
		{http.MethodDelete, "/v1/optouts", "compliance", "write"},
		{http.MethodGet, "/v1/unknown", "api", "read"},
		{http.MethodPost, "/v1/unknown", "api", "write"},
	}
	for _, c := range cases {
		var gotRes, gotAct string
		s := newTestServer(t)
		s.Auth = &httpapi.AuthConfig{
			Keys: httpapi.AuthenticatorFunc(func(string) (rbac.Principal, error) {
				return rbac.Principal{ID: "p", TenantID: "t", Scopes: []string{"*"}}, nil
			}),
			Policy: httpapi.AuthorizerFunc(func(_ rbac.Principal, res, act string) rbac.Decision {
				gotRes, gotAct = res, act
				return rbac.Decision{Allowed: true}
			}),
		}
		req := httptest.NewRequest(c.method, c.path, nil)
		req.Header.Set("Authorization", "Bearer sk_test")
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if gotRes != c.wantRes || gotAct != c.wantAct {
			t.Errorf("%s %s => %s:%s want %s:%s", c.method, c.path, gotRes, gotAct, c.wantRes, c.wantAct)
		}
	}
}

func TestPrincipalInContext(t *testing.T) {
	keys := rbac.NewKeyStore()
	k, secret, err := keys.Create("tenant-a", "ctx", []string{"*"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	var got rbac.Principal
	st := storage.NewMemory()
	svc := service.New(service.Config{Store: st})
	s := httpapi.New(svc, st)
	s.Auth = &httpapi.AuthConfig{Keys: httpapi.KeyStoreAuth(keys)}
	// Capture the principal the middleware injects by wrapping the handler.
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = httpapi.PrincipalFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	})
	// Serve a request through the middleware chain with an inner handler.
	req := httptest.NewRequest(http.MethodGet, "/v1/messages", strings.NewReader(""))
	req.Header.Set("Authorization", "Bearer "+secret)
	rec := httptest.NewRecorder()
	s.Wrap(h).ServeHTTP(rec, req)
	if got.ID != k.ID {
		t.Fatalf("principal ID=%q want %q", got.ID, k.ID)
	}
	if got.TenantID != "tenant-a" {
		t.Fatalf("principal tenant=%q want tenant-a", got.TenantID)
	}
}
