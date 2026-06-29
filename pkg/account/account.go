package account

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"time"
)

var ErrNotFound = errors.New("not found")
var ErrDisabled = errors.New("disabled")
var ErrInvalidCredential = errors.New("invalid credential")

type User struct {
	ID             string
	TenantID       string
	Username       string
	DisplayName    string
	Roles          []string
	Enabled        bool
	QuotaPerMinute int
	Meta           map[string]string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type APIKey struct {
	ID         string
	TenantID   string
	UserID     string
	Name       string
	Prefix     string
	SecretHash string
	Enabled    bool
	ExpiresAt  time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
	LastUsedAt time.Time
	Meta       map[string]string
}

type ProviderCredential struct {
	ID         string
	TenantID   string
	ProviderID string
	SystemID   string
	Password   string
	SystemType string
	BindMode   string
	Enabled    bool
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Meta       map[string]string
}

type Store interface {
	SaveUser(context.Context, User) error
	GetUser(context.Context, string) (User, error)
	ListUsers(context.Context, string) ([]User, error)
	SaveAPIKey(context.Context, APIKey) error
	AuthenticateAPIKey(context.Context, string) (User, APIKey, error)
	SaveProviderCredential(context.Context, ProviderCredential) error
	GetProviderCredential(context.Context, string, string) (ProviderCredential, error)
	ListProviderCredentials(context.Context, string) ([]ProviderCredential, error)
}

type Memory struct {
	mu            sync.RWMutex
	users         map[string]User
	keys          map[string]APIKey
	providerCreds map[string]ProviderCredential
}

func NewMemory() *Memory {
	return &Memory{users: map[string]User{}, keys: map[string]APIKey{}, providerCreds: map[string]ProviderCredential{}}
}

func nowUser(u User) User {
	t := time.Now()
	if u.CreatedAt.IsZero() {
		u.CreatedAt = t
	}
	u.UpdatedAt = t
	if !u.Enabled && u.ID != "" { /* keep explicit disabled */
	}
	return u
}
func (m *Memory) SaveUser(ctx context.Context, u User) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if u.ID == "" {
		u.ID = "user-" + time.Now().Format("20060102150405.000000000")
	}
	if u.TenantID == "" {
		u.TenantID = u.ID
	}
	if u.Meta == nil {
		u.Meta = map[string]string{}
	}
	if !u.Enabled {
		if old, ok := m.users[u.ID]; ok {
			u.Enabled = old.Enabled
		} else {
			u.Enabled = true
		}
	}
	m.users[u.ID] = nowUser(u)
	return nil
}
func (m *Memory) GetUser(ctx context.Context, id string) (User, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	u, ok := m.users[id]
	if !ok {
		return User{}, ErrNotFound
	}
	cp := u
	cp.Roles = append([]string(nil), u.Roles...)
	cp.Meta = copyMap(u.Meta)
	return cp, nil
}
func (m *Memory) ListUsers(ctx context.Context, tenant string) ([]User, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]User, 0, len(m.users))
	for _, u := range m.users {
		if tenant == "" || u.TenantID == tenant {
			cp := u
			cp.Roles = append([]string(nil), u.Roles...)
			cp.Meta = copyMap(u.Meta)
			out = append(out, cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func NewAPIKey(tenantID, userID, name string) (APIKey, string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return APIKey{}, "", err
	}
	secret := "sk_" + hex.EncodeToString(b)
	hash := HashSecret(secret)
	return APIKey{ID: "key-" + time.Now().Format("20060102150405.000000000"), TenantID: tenantID, UserID: userID, Name: name, Prefix: secret[:10], SecretHash: hash, Enabled: true, CreatedAt: time.Now(), UpdatedAt: time.Now()}, secret, nil
}
func HashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}
func EqualSecretHash(secret, hash string) bool {
	hs := HashSecret(secret)
	return hmac.Equal([]byte(hs), []byte(hash))
}
func (m *Memory) SaveAPIKey(ctx context.Context, k APIKey) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if k.ID == "" {
		k.ID = "key-" + time.Now().Format("20060102150405.000000000")
	}
	if k.Meta == nil {
		k.Meta = map[string]string{}
	}
	if k.CreatedAt.IsZero() {
		k.CreatedAt = time.Now()
	}
	k.UpdatedAt = time.Now()
	m.keys[k.ID] = k
	return nil
}
func (m *Memory) AuthenticateAPIKey(ctx context.Context, secret string) (User, APIKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, k := range m.keys {
		if !k.Enabled {
			continue
		}
		if !k.ExpiresAt.IsZero() && time.Now().After(k.ExpiresAt) {
			continue
		}
		if EqualSecretHash(secret, k.SecretHash) {
			u, ok := m.users[k.UserID]
			if !ok {
				return User{}, APIKey{}, ErrNotFound
			}
			if !u.Enabled {
				return User{}, APIKey{}, ErrDisabled
			}
			k.LastUsedAt = time.Now()
			k.UpdatedAt = time.Now()
			m.keys[id] = k
			return u, k, nil
		}
	}
	return User{}, APIKey{}, ErrInvalidCredential
}

func (m *Memory) SaveProviderCredential(ctx context.Context, c ProviderCredential) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c.ID == "" {
		c.ID = c.TenantID + ":" + c.ProviderID
	}
	if c.Meta == nil {
		c.Meta = map[string]string{}
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now()
	}
	c.UpdatedAt = time.Now()
	if !c.Enabled {
		if old, ok := m.providerCreds[c.ID]; ok {
			c.Enabled = old.Enabled
		} else {
			c.Enabled = true
		}
	}
	m.providerCreds[c.ID] = c
	return nil
}
func (m *Memory) GetProviderCredential(ctx context.Context, tenantID, providerID string) (ProviderCredential, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.providerCreds[tenantID+":"+providerID]
	if !ok {
		return ProviderCredential{}, ErrNotFound
	}
	if !c.Enabled {
		return ProviderCredential{}, ErrDisabled
	}
	cp := c
	cp.Meta = copyMap(c.Meta)
	return cp, nil
}
func (m *Memory) ListProviderCredentials(ctx context.Context, tenantID string) ([]ProviderCredential, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []ProviderCredential{}
	for _, c := range m.providerCreds {
		if tenantID == "" || c.TenantID == tenantID {
			cp := c
			cp.Meta = copyMap(c.Meta)
			out = append(out, cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func RedactProviderCredential(c ProviderCredential) ProviderCredential {
	c.Password = "***redacted***"
	return c
}
func copyMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
