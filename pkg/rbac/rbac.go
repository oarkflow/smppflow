package rbac

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

type Role string

const (
	RoleAdmin       Role = "admin"
	RoleTenantAdmin Role = "tenant_admin"
	RoleSupport     Role = "support"
	RoleFinance     Role = "finance"
	RoleCompliance  Role = "compliance"
	RoleReadOnly    Role = "readonly"
)

type Principal struct {
	ID       string
	TenantID string
	Roles    []Role
	Scopes   []string
}
type Permission struct {
	Resource     string
	Action       string
	TenantScoped bool
}
type Decision struct {
	Allowed bool
	Reason  string
}

type Policy struct {
	mu    sync.RWMutex
	perms map[Role][]Permission
}

func NewPolicy() *Policy {
	p := &Policy{perms: map[Role][]Permission{}}
	p.Allow(RoleAdmin, "*", "*", false)
	p.Allow(RoleReadOnly, "*", "read", true)
	p.Allow(RoleSupport, "message", "read", true)
	p.Allow(RoleSupport, "trace", "read", true)
	p.Allow(RoleFinance, "billing", "read", true)
	p.Allow(RoleCompliance, "compliance", "*", true)
	p.Allow(RoleTenantAdmin, "tenant", "*", true)
	p.Allow(RoleTenantAdmin, "message", "*", true)
	return p
}
func (p *Policy) Allow(role Role, resource, action string, scoped bool) {
	p.mu.Lock()
	p.perms[role] = append(p.perms[role], Permission{resource, action, scoped})
	p.mu.Unlock()
}
func (p *Policy) Check(pr Principal, tenant, resource, action string) Decision {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, s := range pr.Scopes {
		if match(s, resource+":"+action) || match(s, resource+":*") || s == "*" {
			if tenant == "" || pr.TenantID == "" || tenant == pr.TenantID {
				return Decision{Allowed: true}
			}
		}
	}
	for _, role := range pr.Roles {
		for _, perm := range p.perms[role] {
			if match(perm.Resource, resource) && match(perm.Action, action) {
				if perm.TenantScoped && tenant != "" && pr.TenantID != "" && tenant != pr.TenantID {
					return Decision{Allowed: false, Reason: "tenant isolation denied"}
				}
				return Decision{Allowed: true}
			}
		}
	}
	return Decision{Allowed: false, Reason: "permission denied"}
}
func match(pattern, v string) bool { return pattern == "*" || strings.EqualFold(pattern, v) }

type APIKey struct {
	ID         string
	TenantID   string
	Name       string
	Hash       string
	Scopes     []string
	Enabled    bool
	CreatedAt  time.Time
	ExpiresAt  time.Time
	LastUsedAt time.Time
}
type KeyStore struct {
	mu   sync.RWMutex
	keys map[string]APIKey
}

func NewKeyStore() *KeyStore { return &KeyStore{keys: map[string]APIKey{}} }
func (s *KeyStore) Create(tenant, name string, scopes []string, ttl time.Duration) (APIKey, string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return APIKey{}, "", err
	}
	secret := "sk_" + hex.EncodeToString(raw)
	h := hash(secret)
	id := "key_" + secret[len(secret)-12:]
	k := APIKey{ID: id, TenantID: tenant, Name: name, Hash: h, Scopes: append([]string(nil), scopes...), Enabled: true, CreatedAt: time.Now()}
	if ttl > 0 {
		k.ExpiresAt = k.CreatedAt.Add(ttl)
	}
	s.mu.Lock()
	s.keys[id] = k
	s.mu.Unlock()
	return k, secret, nil
}
func (s *KeyStore) Authenticate(secret string) (Principal, error) {
	h := hash(secret)
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, k := range s.keys {
		if !k.Enabled {
			continue
		}
		if !k.ExpiresAt.IsZero() && now.After(k.ExpiresAt) {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(k.Hash), []byte(h)) == 1 {
			k.LastUsedAt = now
			s.keys[id] = k
			return Principal{ID: k.ID, TenantID: k.TenantID, Scopes: append([]string(nil), k.Scopes...)}, nil
		}
	}
	return Principal{}, errors.New("invalid api key")
}
func (s *KeyStore) Disable(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.keys[id]
	if !ok {
		return errors.New("key not found")
	}
	k.Enabled = false
	s.keys[id] = k
	return nil
}
func (s *KeyStore) List(tenant string) []APIKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []APIKey{}
	for _, k := range s.keys {
		if tenant == "" || k.TenantID == tenant {
			k.Hash = ""
			out = append(out, k)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func hash(s string) string { b := sha256.Sum256([]byte(s)); return hex.EncodeToString(b[:]) }
