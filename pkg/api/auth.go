package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/rbac"
	"github.com/oarkflow/smppflow/pkg/security"
)

// Authenticator resolves a presented API key secret to a Principal.
// rbac.KeyStore and account-backed stores satisfy this via adapters below.
type Authenticator interface {
	Authenticate(secret string) (rbac.Principal, error)
}

// AuthenticatorFunc adapts a plain function to Authenticator.
type AuthenticatorFunc func(secret string) (rbac.Principal, error)

func (f AuthenticatorFunc) Authenticate(secret string) (rbac.Principal, error) { return f(secret) }

// Authorizer decides whether a Principal may perform action on resource.
type Authorizer interface {
	Authorize(p rbac.Principal, resource, action string) rbac.Decision
}

// AuthorizerFunc adapts a plain function to Authorizer.
type AuthorizerFunc func(p rbac.Principal, resource, action string) rbac.Decision

func (f AuthorizerFunc) Authorize(p rbac.Principal, resource, action string) rbac.Decision {
	return f(p, resource, action)
}

// AuthConfig configures authentication and authorization for the HTTP API.
//
// When Keys is non-nil every request except those matching Public must present
// a valid API key. When Keys is nil the API is intentionally unauthenticated and
// RequireAuth reports false, which is only appropriate for local development.
type AuthConfig struct {
	// Keys authenticates API key secrets. Required to enforce authentication.
	Keys Authenticator
	// Policy authorizes authenticated principals. When nil, authentication is
	// enforced but every authenticated principal is allowed.
	Policy Authorizer
	// Public lists exact request paths that skip authentication entirely.
	// Defaults to /healthz when empty.
	Public []string
	// IPPolicy optionally restricts callers by remote address.
	IPPolicy *security.IPPolicy
	// MaxRequestsPerMinute rate-limits each principal. Zero disables limiting.
	MaxRequestsPerMinute int
	// TrustProxyHeader enables reading the client IP from X-Forwarded-For.
	// Only enable behind a proxy you control, otherwise callers can spoof it.
	TrustProxyHeader bool
}

// RequireAuth reports whether requests must be authenticated.
func (c *AuthConfig) RequireAuth() bool { return c != nil && c.Keys != nil }

type ctxKey int

const (
	ctxKeyPrincipal ctxKey = iota
)

// PrincipalFromContext returns the authenticated principal, if any.
func PrincipalFromContext(ctx context.Context) (rbac.Principal, bool) {
	p, ok := ctx.Value(ctxKeyPrincipal).(rbac.Principal)
	return p, ok
}

type rateWindow struct {
	mu     sync.Mutex
	count  int
	window time.Time
}

// middleware authenticates, authorizes and rate-limits requests before they
// reach the route handlers. It also overwrites X-Actor-ID so audit records are
// attributed to the authenticated principal rather than a client-supplied value.
func (a *Server) middleware(next http.Handler) http.Handler {
	cfg := a.Auth
	if !cfg.RequireAuth() {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
		})
	}
	public := map[string]bool{}
	for _, p := range cfg.Public {
		public[p] = true
	}
	if len(public) == 0 {
		public["/healthz"] = true
	}
	limits := sync.Map{}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if public[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		if cfg.IPPolicy != nil {
			remote := r.RemoteAddr
			if cfg.TrustProxyHeader {
				if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
					remote = strings.TrimSpace(strings.Split(xff, ",")[0])
				}
			}
			if !cfg.IPPolicy.Allowed(remote) {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
		}
		secret := extractSecret(r)
		if secret == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="smppflow"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		principal, err := cfg.Keys.Authenticate(secret)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="smppflow", error="invalid_token"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if cfg.MaxRequestsPerMinute > 0 && !allow(&limits, principal.ID, cfg.MaxRequestsPerMinute) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		if cfg.Policy != nil {
			resource, action := resolveAction(r.Method, r.URL.Path)
			if d := cfg.Policy.Authorize(principal, resource, action); !d.Allowed {
				http.Error(w, "forbidden: "+d.Reason, http.StatusForbidden)
				return
			}
		}
		r = r.WithContext(context.WithValue(r.Context(), ctxKeyPrincipal, principal))
		// Audit trail must reflect the authenticated principal, never a header
		// the caller controls.
		if principal.ID != "" {
			r.Header.Set("X-Actor-ID", principal.ID)
		}
		if principal.TenantID != "" && r.URL.Query().Get("tenant_id") == "" {
			q := r.URL.Query()
			q.Set("tenant_id", principal.TenantID)
			r.URL.RawQuery = q.Encode()
		}
		next.ServeHTTP(w, r)
	})
}

// extractSecret pulls the API key from the Authorization header (Bearer or
// Basic-style raw value) or the X-API-Key header.
func extractSecret(r *http.Request) string {
	if h := r.Header.Get("X-API-Key"); h != "" {
		return strings.TrimSpace(h)
	}
	h := strings.TrimSpace(r.Header.Get("Authorization"))
	if h == "" {
		return ""
	}
	const bearer = "bearer "
	if len(h) > len(bearer) && strings.EqualFold(h[:len(bearer)], bearer) {
		return strings.TrimSpace(h[len(bearer):])
	}
	return h
}

// resolveAction maps an HTTP method and path to an rbac resource and action.
// Unknown paths degrade to api:write, which is the safer default.
func resolveAction(method, path string) (resource, action string) {
	action = "read"
	if method != http.MethodGet && method != http.MethodHead {
		action = "write"
	}
	if r, ok := routeResources[path]; ok {
		return r, action
	}
	if strings.HasPrefix(path, "/v1/reports/") {
		return "report", "read"
	}
	return "api", action
}

// routeResources maps exact API paths to rbac resources. Paths not listed fall
// back to the "api" resource.
var routeResources = map[string]string{
	"/v1/messages/send":            "message",
	"/v1/messages/enqueue":         "message",
	"/v1/messages":                 "message",
	"/v1/dlrs":                     "message",
	"/v1/reports/summary":          "report",
	"/v1/reports/delivery":         "report",
	"/v1/reports/providers":        "report",
	"/v1/reports/tenants":          "report",
	"/v1/reports/errors":           "report",
	"/v1/reports/export.ndjson":    "report",
	"/v1/manager/queue":            "queue",
	"/v1/manager/dlq":              "queue",
	"/v1/manager/queue/replay-dlq": "queue",
	"/v1/manager/queue/cancel":     "queue",
	"/v1/manager/route/simulate":   "route",
	"/v1/manager/messages/trace":   "trace",
	"/v1/manager/tenants":          "tenant",
	"/v1/manager/providers":        "provider",
	"/v1/manager/binds":            "bind",
	"/v1/manager/senders":          "sender",
	"/v1/manager/sessions":         "session",
	"/v1/manager/audit":            "audit",
	"/v1/optouts":                  "compliance",
	"/v1/contacts":                 "contact",
	"/v1/ratelimits":               "ratelimit",
	"/v1/alerts/open":              "alert",
	"/v1/manager/api-keys":         "apikey",
	"/healthz":                     "health",
}

// allow implements a fixed one-minute window rate limit per principal ID.
func allow(m *sync.Map, id string, perMinute int) bool {
	v, _ := m.LoadOrStore(id, &rateWindow{})
	w := v.(*rateWindow)
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	if now.Sub(w.window) >= time.Minute {
		w.window = now
		w.count = 0
	}
	if w.count >= perMinute {
		return false
	}
	w.count++
	return true
}

// ErrUnauthenticated is returned by Authenticator implementations when a key is
// missing, unknown, disabled or expired.
var ErrUnauthenticated = errors.New("unauthorized")

// KeyStoreAuth adapts an rbac.KeyStore to Authenticator.
func KeyStoreAuth(s *rbac.KeyStore) Authenticator {
	return AuthenticatorFunc(func(secret string) (rbac.Principal, error) {
		if s == nil {
			return rbac.Principal{}, ErrUnauthenticated
		}
		p, err := s.Authenticate(secret)
		if err != nil {
			return rbac.Principal{}, ErrUnauthenticated
		}
		return p, nil
	})
}

// PolicyAuthorize adapts an rbac.Policy to Authorizer. Tenant isolation is
// enforced against the principal's own tenant.
func PolicyAuthorize(p *rbac.Policy) Authorizer {
	return AuthorizerFunc(func(pr rbac.Principal, resource, action string) rbac.Decision {
		if p == nil {
			return rbac.Decision{Allowed: true}
		}
		return p.Check(pr, pr.TenantID, resource, action)
	})
}
