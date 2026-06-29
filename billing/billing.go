package billing

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/message"
)

type Money struct {
	Currency     string
	AmountMicros int64
}

func NewMoney(cur string, amount float64) Money {
	return Money{Currency: cur, AmountMicros: int64(math.Round(amount * 1_000_000))}
}
func (m Money) Float() float64 { return float64(m.AmountMicros) / 1_000_000 }
func (m Money) Add(x Money) (Money, error) {
	if m.Currency != x.Currency {
		return m, errors.New("currency mismatch")
	}
	m.AmountMicros += x.AmountMicros
	return m, nil
}

type PriceRule struct {
	ID                   string
	TenantID             string
	ProviderID           string
	Country              string
	MCC                  string
	MNC                  string
	Currency             string
	CostPerSegmentMicros int64
	SellPerSegmentMicros int64
	EffectiveFrom        time.Time
	EffectiveTo          time.Time
}
type RatedMessage struct {
	MessageID  message.ID
	TenantID   string
	ProviderID string
	Country    string
	MCC        string
	MNC        string
	Segments   int
	Cost       Money
	Sell       Money
	Margin     Money
	RatedAt    time.Time
}
type FXTable map[string]float64

func (fx FXTable) Convert(m Money, to string) (Money, error) {
	if m.Currency == to {
		return m, nil
	}
	fromRate := fx[m.Currency]
	toRate := fx[to]
	if fromRate == 0 || toRate == 0 {
		return m, fmt.Errorf("missing fx rate %s/%s", m.Currency, to)
	}
	usd := m.Float() / fromRate
	return NewMoney(to, usd*toRate), nil
}

type Engine struct {
	mu           sync.RWMutex
	rules        []PriceRule
	balances     map[string]Money
	reservations map[string]Money
}

func NewEngine() *Engine {
	return &Engine{balances: map[string]Money{}, reservations: map[string]Money{}}
}
func (e *Engine) AddRule(r PriceRule) { e.mu.Lock(); e.rules = append(e.rules, r); e.mu.Unlock() }
func (e *Engine) SetBalance(tenant string, m Money) {
	e.mu.Lock()
	e.balances[tenant] = m
	e.mu.Unlock()
}
func (e *Engine) Rate(ctx context.Context, msg message.Message, providerID, country, mcc, mnc string, segments int) (RatedMessage, error) {
	_ = ctx
	if segments <= 0 {
		segments = len(msg.Parts)
		if segments <= 0 {
			segments = 1
		}
	}
	e.mu.RLock()
	rule, ok := e.match(msg.TenantID, providerID, country, mcc, mnc, time.Now())
	e.mu.RUnlock()
	if !ok {
		return RatedMessage{}, errors.New("no price rule matched")
	}
	cost := Money{rule.Currency, rule.CostPerSegmentMicros * int64(segments)}
	sell := Money{rule.Currency, rule.SellPerSegmentMicros * int64(segments)}
	return RatedMessage{MessageID: msg.ID, TenantID: msg.TenantID, ProviderID: providerID, Country: country, MCC: mcc, MNC: mnc, Segments: segments, Cost: cost, Sell: sell, Margin: Money{rule.Currency, sell.AmountMicros - cost.AmountMicros}, RatedAt: time.Now()}, nil
}
func (e *Engine) Reserve(ctx context.Context, id string, r RatedMessage) error {
	_ = ctx
	e.mu.Lock()
	defer e.mu.Unlock()
	bal := e.balances[r.TenantID]
	if bal.Currency == "" {
		return errors.New("tenant balance not found")
	}
	if bal.Currency != r.Sell.Currency {
		return errors.New("balance currency mismatch")
	}
	if bal.AmountMicros < r.Sell.AmountMicros {
		return errors.New("insufficient balance")
	}
	bal.AmountMicros -= r.Sell.AmountMicros
	e.balances[r.TenantID] = bal
	e.reservations[id] = r.Sell
	return nil
}
func (e *Engine) Capture(ctx context.Context, tenant, id string) error {
	_ = ctx
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.reservations, id)
	return nil
}
func (e *Engine) Release(ctx context.Context, tenant, id string) error {
	_ = ctx
	e.mu.Lock()
	defer e.mu.Unlock()
	m, ok := e.reservations[id]
	if !ok {
		return nil
	}
	bal := e.balances[tenant]
	if bal.Currency == m.Currency {
		bal.AmountMicros += m.AmountMicros
		e.balances[tenant] = bal
	}
	delete(e.reservations, id)
	return nil
}
func (e *Engine) match(tenant, provider, country, mcc, mnc string, now time.Time) (PriceRule, bool) {
	var best PriceRule
	score := -1
	for _, r := range e.rules {
		if r.EffectiveFrom.After(now) || (!r.EffectiveTo.IsZero() && r.EffectiveTo.Before(now)) {
			continue
		}
		s := 0
		if r.TenantID != "" {
			if r.TenantID != tenant {
				continue
			}
			s += 8
		}
		if r.ProviderID != "" {
			if r.ProviderID != provider {
				continue
			}
			s += 4
		}
		if r.Country != "" {
			if r.Country != country {
				continue
			}
			s += 2
		}
		if r.MCC != "" {
			if r.MCC != mcc {
				continue
			}
			s += 1
		}
		if r.MNC != "" {
			if r.MNC != mnc {
				continue
			}
			s += 1
		}
		if s > score {
			score = s
			best = r
		}
	}
	return best, score >= 0
}
func ExportCSV(w io.Writer, rows []RatedMessage) error {
	c := csv.NewWriter(w)
	_ = c.Write([]string{"message_id", "tenant_id", "provider_id", "country", "mcc", "mnc", "segments", "currency", "cost", "sell", "margin", "rated_at"})
	sort.Slice(rows, func(i, j int) bool { return rows[i].RatedAt.Before(rows[j].RatedAt) })
	for _, r := range rows {
		_ = c.Write([]string{string(r.MessageID), r.TenantID, r.ProviderID, r.Country, r.MCC, r.MNC, fmt.Sprint(r.Segments), r.Sell.Currency, fmt.Sprintf("%.6f", r.Cost.Float()), fmt.Sprintf("%.6f", r.Sell.Float()), fmt.Sprintf("%.6f", r.Margin.Float()), r.RatedAt.Format(time.RFC3339)})
	}
	c.Flush()
	return c.Error()
}
