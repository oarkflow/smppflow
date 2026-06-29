package secrets

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Ciphertext struct {
	KeyID     string    `json:"key_id"`
	Nonce     string    `json:"nonce"`
	Data      string    `json:"data"`
	CreatedAt time.Time `json:"created_at"`
}
type KMS interface {
	Encrypt(context.Context, []byte) (Ciphertext, error)
	Decrypt(context.Context, Ciphertext) ([]byte, error)
	Rotate(context.Context) (string, error)
}

type LocalKMS struct {
	mu     sync.RWMutex
	keys   map[string][]byte
	active string
}

func NewLocalKMS(master []byte) *LocalKMS {
	k := &LocalKMS{keys: map[string][]byte{}}
	if len(master) == 0 {
		master = randBytes(32)
	}
	h := sha256.Sum256(master)
	k.active = "local-1"
	k.keys[k.active] = h[:]
	return k
}
func (k *LocalKMS) Encrypt(ctx context.Context, plain []byte) (Ciphertext, error) {
	_ = ctx
	k.mu.RLock()
	key := append([]byte(nil), k.keys[k.active]...)
	id := k.active
	k.mu.RUnlock()
	block, err := aes.NewCipher(key)
	if err != nil {
		return Ciphertext{}, err
	}
	g, err := cipher.NewGCM(block)
	if err != nil {
		return Ciphertext{}, err
	}
	nonce := randBytes(g.NonceSize())
	data := g.Seal(nil, nonce, plain, nil)
	return Ciphertext{KeyID: id, Nonce: base64.StdEncoding.EncodeToString(nonce), Data: base64.StdEncoding.EncodeToString(data), CreatedAt: time.Now()}, nil
}
func (k *LocalKMS) Decrypt(ctx context.Context, ct Ciphertext) ([]byte, error) {
	_ = ctx
	k.mu.RLock()
	key := append([]byte(nil), k.keys[ct.KeyID]...)
	k.mu.RUnlock()
	if len(key) == 0 {
		return nil, fmt.Errorf("unknown key id %s", ct.KeyID)
	}
	nonce, err := base64.StdEncoding.DecodeString(ct.Nonce)
	if err != nil {
		return nil, err
	}
	data, err := base64.StdEncoding.DecodeString(ct.Data)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	g, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return g.Open(nil, nonce, data, nil)
}
func (k *LocalKMS) Rotate(ctx context.Context) (string, error) {
	_ = ctx
	k.mu.Lock()
	defer k.mu.Unlock()
	id := fmt.Sprintf("local-%d", len(k.keys)+1)
	k.keys[id] = randBytes(32)
	k.active = id
	return id, nil
}

type VaultKMS struct {
	Address    string
	Token      string
	TransitKey string
	Client     *http.Client
}

func (v VaultKMS) Encrypt(ctx context.Context, p []byte) (Ciphertext, error) {
	if v.Address == "" || v.TransitKey == "" {
		return Ciphertext{}, errors.New("vault address and transit key required")
	}
	payload := map[string]string{"plaintext": base64.StdEncoding.EncodeToString(p)}
	var out struct {
		Data struct {
			Ciphertext string `json:"ciphertext"`
		} `json:"data"`
	}
	if err := v.do(ctx, "encrypt", payload, &out); err != nil {
		return Ciphertext{}, err
	}
	return Ciphertext{KeyID: "vault:" + v.TransitKey, Data: out.Data.Ciphertext, CreatedAt: time.Now()}, nil
}
func (v VaultKMS) Decrypt(ctx context.Context, ct Ciphertext) ([]byte, error) {
	payload := map[string]string{"ciphertext": ct.Data}
	var out struct {
		Data struct {
			Plaintext string `json:"plaintext"`
		} `json:"data"`
	}
	if err := v.do(ctx, "decrypt", payload, &out); err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(out.Data.Plaintext)
}
func (v VaultKMS) Rotate(ctx context.Context) (string, error) {
	var out map[string]any
	err := v.do(ctx, "rotate", map[string]string{}, &out)
	return "vault:" + v.TransitKey, err
}
func (v VaultKMS) do(ctx context.Context, op string, payload any, out any) error {
	b, _ := json.Marshal(payload)
	url := strings.TrimRight(v.Address, "/") + "/v1/transit/" + op + "/" + v.TransitKey
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if v.Token != "" {
		req.Header.Set("X-Vault-Token", v.Token)
	}
	c := v.Client
	if c == nil {
		c = http.DefaultClient
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("vault %s failed: %s %s", op, resp.Status, string(data))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type SecretStore struct {
	mu       sync.RWMutex
	kms      KMS
	records  map[string]Ciphertext
	versions map[string]int
}

func NewStore(k KMS) *SecretStore {
	return &SecretStore{kms: k, records: map[string]Ciphertext{}, versions: map[string]int{}}
}
func (s *SecretStore) Put(ctx context.Context, name string, value []byte) error {
	if name == "" {
		return errors.New("secret name required")
	}
	ct, err := s.kms.Encrypt(ctx, value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.records[name] = ct
	s.versions[name]++
	s.mu.Unlock()
	return nil
}
func (s *SecretStore) Get(ctx context.Context, name string) ([]byte, error) {
	s.mu.RLock()
	ct, ok := s.records[name]
	s.mu.RUnlock()
	if !ok {
		return nil, os.ErrNotExist
	}
	return s.kms.Decrypt(ctx, ct)
}
func (s *SecretStore) Rotate(ctx context.Context, name string, newValue []byte) error {
	if _, err := s.kms.Rotate(ctx); err != nil {
		return err
	}
	return s.Put(ctx, name, newValue)
}
func (s *SecretStore) ExportEncrypted() map[string]Ciphertext {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[string]Ciphertext{}
	for k, v := range s.records {
		out[k] = v
	}
	return out
}
func Redact(s string) string {
	if len(s) <= 4 {
		return "****"
	}
	return s[:2] + strings.Repeat("*", len(s)-4) + s[len(s)-2:]
}
func randBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}
