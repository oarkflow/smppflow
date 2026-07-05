package routing

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/compliance"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/provider"
)

type denyCompliance struct{}

func (denyCompliance) Evaluate(ctx context.Context, req message.SubmitRequest, country string) compliance.RuleDecision {
	return compliance.RuleDecision{Allowed: false, Reasons: []string{"blocked"}}
}

type fixedRating struct{ sell int64 }

func (f fixedRating) RateRoute(ctx context.Context, req message.SubmitRequest, providerID string, segments int) (RouteRate, error) {
	return RouteRate{ProviderID: providerID, Currency: "USD", SellMicros: f.sell, CostMicros: f.sell / 2, MarginMicros: f.sell / 2, Billable: true}, nil
}

type denyBalance struct{}

func (denyBalance) CanReserve(ctx context.Context, tenant string, rate RouteRate) error {
	return context.Canceled
}

func productionRouter() *Router {
	r := New()
	_ = r.RegisterProvider(provider.Generic34("p1", "127.0.0.1:2775", "u", "p"))
	_ = r.RegisterRule(Rule{ID: "r1", TenantID: "t1", Prefix: "977", ProviderID: "p1", Priority: 1, Weight: 1, Country: "NP"})
	return r
}

func TestProductionPlanIsDryRun(t *testing.T) {
	r := productionRouter()
	plan, err := r.Simulate(context.Background(), message.SubmitRequest{TenantID: "t1", From: "INFO", To: "9779841111111", Text: "hello", Meta: map[string]string{"country": "NP"}})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Selected == nil || plan.Selected.ProviderID != "p1" {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	if got := len(r.Snapshots()); got != 0 {
		t.Fatalf("simulate should not create snapshot, got %d", got)
	}
}

func TestRequireActiveSession(t *testing.T) {
	r := productionRouter()
	r.RequireActiveSession(true)
	_, err := r.Route(context.Background(), message.SubmitRequest{TenantID: "t1", From: "INFO", To: "9779841111111", Text: "hello", Meta: map[string]string{"country": "NP"}})
	if err == nil {
		t.Fatal("expected no route without active session")
	}
	r.UpdateSessionSnapshot(SessionSnapshot{ProviderID: "p1", Healthy: true, State: "bound", WindowSize: 10})
	if _, err := r.Route(context.Background(), message.SubmitRequest{TenantID: "t1", From: "INFO", To: "9779841111111", Text: "hello", Meta: map[string]string{"country": "NP"}}); err != nil {
		t.Fatal(err)
	}
}

func TestComplianceRatingBalanceFilters(t *testing.T) {
	req := message.SubmitRequest{TenantID: "t1", From: "INFO", To: "9779841111111", Text: "hello", Meta: map[string]string{"country": "NP"}}
	r := productionRouter()
	r.SetComplianceFilter(denyCompliance{})
	if _, err := r.Route(context.Background(), req); err == nil {
		t.Fatal("expected compliance rejection")
	}
	r = productionRouter()
	r.SetRatingFilter(fixedRating{sell: 5000})
	r.SetBalanceFilter(denyBalance{})
	if _, err := r.Route(context.Background(), req); err == nil {
		t.Fatal("expected balance rejection")
	}
}

func TestRouteRepositoryRoundTrip(t *testing.T) {
	r := productionRouter()
	path := filepath.Join(t.TempDir(), "routes.json")
	r.SetRouteRepository(NewFileRouteRepository(path))
	if err := r.SaveRoutes(context.Background()); err != nil {
		t.Fatal(err)
	}
	r2 := New()
	r2.SetRouteRepository(NewFileRouteRepository(path))
	if err := r2.LoadRoutes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(r2.Rules()) != 1 || len(r2.Providers()) != 1 {
		t.Fatalf("load mismatch rules=%d providers=%d", len(r2.Rules()), len(r2.Providers()))
	}
}

func TestCircuitBreakerRejectsOpen(t *testing.T) {
	r := productionRouter()
	cb := NewCircuitBreaker()
	cb.FailureThreshold = 1
	cb.OpenDuration = time.Minute
	r.SetCircuitBreaker(cb)
	cb.MarkFailure("p1|NP||")
	_, err := r.Route(context.Background(), message.SubmitRequest{TenantID: "t1", From: "INFO", To: "9779841111111", Text: "hello", Meta: map[string]string{"country": "NP"}})
	if err == nil {
		t.Fatal("expected circuit rejection")
	}
}

func TestBCLRouteLoader(t *testing.T) {
	r := New()
	cfg := []byte(`
provider "p1" {
  addr "127.0.0.1:2775"
  system_id "sys"
  password "pass"
  tps 10
}
route "np_otp" {
  tenant "t1"
  country "NP"
  prefix "977"
  provider "p1"
  message_type "otp"
  priority 1
}`)
	if err := r.LoadBCLRoutes(cfg); err != nil {
		t.Fatal(err)
	}
	_, err := r.Route(context.Background(), message.SubmitRequest{TenantID: "t1", From: "INFO", To: "9779841111111", Text: "hello", Meta: map[string]string{"country": "NP", "message_type": "otp"}})
	if err != nil {
		t.Fatal(err)
	}
}
