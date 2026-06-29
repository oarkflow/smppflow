package lookup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	phone "github.com/oarkflow/phone"
)

type Result struct {
	Number          string         `json:"number"`
	E164            string         `json:"e164"`
	Country         string         `json:"country"`
	MCC             string         `json:"mcc"`
	MNC             string         `json:"mnc"`
	NNC             string         `json:"nnc"`
	Network         string         `json:"network"`
	OriginalCarrier string         `json:"original_carrier"`
	CurrentCarrier  string         `json:"current_carrier"`
	Ported          bool           `json:"ported"`
	Roaming         bool           `json:"roaming"`
	Reachable       bool           `json:"reachable"`
	ActiveSIM       bool           `json:"active_sim"`
	SIMSwapRisk     float64        `json:"sim_swap_risk"`
	FraudScore      float64        `json:"fraud_score"`
	LookupAt        time.Time      `json:"lookup_at"`
	Source          string         `json:"source"`
	Raw             map[string]any `json:"raw,omitempty"`
}
type Adapter interface {
	Lookup(context.Context, string, string) (Result, error)
}
type AdapterFunc func(context.Context, string, string) (Result, error)

func (f AdapterFunc) Lookup(ctx context.Context, n, c string) (Result, error) { return f(ctx, n, c) }

type Chain struct {
	Adapters        []Adapter
	StopOnReachable bool
}

func (c Chain) Lookup(ctx context.Context, number, country string) (Result, error) {
	var out Result
	var errs []error
	for _, a := range c.Adapters {
		r, err := a.Lookup(ctx, number, country)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		out = Merge(out, r)
		if c.StopOnReachable && out.Reachable {
			return out, nil
		}
	}
	if out.Number == "" && len(errs) > 0 {
		return out, errors.Join(errs...)
	}
	return out, nil
}
func Merge(a, b Result) Result {
	if b.Number != "" {
		a.Number = b.Number
	}
	if b.E164 != "" {
		a.E164 = b.E164
	}
	if b.Country != "" {
		a.Country = b.Country
	}
	if b.MCC != "" {
		a.MCC = b.MCC
	}
	if b.MNC != "" {
		a.MNC = b.MNC
	}
	if b.NNC != "" {
		a.NNC = b.NNC
	}
	if b.Network != "" {
		a.Network = b.Network
	}
	if b.OriginalCarrier != "" {
		a.OriginalCarrier = b.OriginalCarrier
	}
	if b.CurrentCarrier != "" {
		a.CurrentCarrier = b.CurrentCarrier
	}
	a.Ported = a.Ported || b.Ported
	a.Roaming = a.Roaming || b.Roaming
	a.Reachable = a.Reachable || b.Reachable
	a.ActiveSIM = a.ActiveSIM || b.ActiveSIM
	if b.SIMSwapRisk > a.SIMSwapRisk {
		a.SIMSwapRisk = b.SIMSwapRisk
	}
	if b.FraudScore > a.FraudScore {
		a.FraudScore = b.FraudScore
	}
	if !b.LookupAt.IsZero() {
		a.LookupAt = b.LookupAt
	}
	if b.Source != "" {
		if a.Source != "" {
			a.Source += ","
		}
		a.Source += b.Source
	}
	if a.Raw == nil {
		a.Raw = map[string]any{}
	}
	for k, v := range b.Raw {
		a.Raw[k] = v
	}
	return a
}

type PhoneAdapter struct{}

func (PhoneAdapter) Lookup(ctx context.Context, number, country string) (Result, error) {
	_ = ctx
	v := phone.Verify(number)
	r := Result{Number: number, LookupAt: time.Now(), Source: "phone"}
	r.Raw = map[string]any{"verified": v}
	if n, err := phone.Parse(number, country); err == nil {
		r.E164 = phone.Format(n, phone.E164)
		r.Country = country
		r.Reachable = true
		r.ActiveSIM = !v.Invalid
	} else if v.Invalid {
		return r, err
	}
	return r, nil
}

type StaticAdapter struct {
	mu   sync.RWMutex
	Data map[string]Result
}

func NewStaticAdapter() *StaticAdapter { return &StaticAdapter{Data: map[string]Result{}} }
func (s *StaticAdapter) Put(e164 string, r Result) {
	s.mu.Lock()
	if r.E164 == "" {
		r.E164 = e164
	}
	s.Data[e164] = r
	s.mu.Unlock()
}
func (s *StaticAdapter) Lookup(ctx context.Context, number, country string) (Result, error) {
	base, _ := PhoneAdapter{}.Lookup(ctx, number, country)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if r, ok := s.Data[base.E164]; ok {
		return Merge(base, r), nil
	}
	if r, ok := s.Data[number]; ok {
		return Merge(base, r), nil
	}
	return base, nil
}

type HTTPAdapter struct {
	Name     string
	Endpoint string
	Token    string
	Client   *http.Client
}

func (h HTTPAdapter) Lookup(ctx context.Context, number, country string) (Result, error) {
	if h.Endpoint == "" {
		return Result{}, errors.New("lookup endpoint required")
	}
	body, _ := json.Marshal(map[string]string{"number": number, "country": country})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.Endpoint, bytes.NewReader(body))
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if h.Token != "" {
		req.Header.Set("Authorization", "Bearer "+h.Token)
	}
	c := h.Client
	if c == nil {
		c = http.DefaultClient
	}
	resp, err := c.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return Result{}, fmt.Errorf("lookup %s returned %s", h.Name, resp.Status)
	}
	var r Result
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return r, err
	}
	if r.Source == "" {
		r.Source = h.Name
	}
	if r.LookupAt.IsZero() {
		r.LookupAt = time.Now()
	}
	return r, nil
}
