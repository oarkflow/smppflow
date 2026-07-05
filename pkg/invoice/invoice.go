package invoice

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/oarkflow/smppflow/pkg/billing"
	"sort"
	"time"
)

type Line struct {
	Description  string `json:"description"`
	Quantity     int    `json:"quantity"`
	UnitMicros   int64  `json:"unit_micros"`
	AmountMicros int64  `json:"amount_micros"`
	Currency     string `json:"currency"`
}
type Invoice struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	PeriodStart    time.Time `json:"period_start"`
	PeriodEnd      time.Time `json:"period_end"`
	Currency       string    `json:"currency"`
	Lines          []Line    `json:"lines"`
	SubtotalMicros int64     `json:"subtotal_micros"`
	TaxMicros      int64     `json:"tax_micros"`
	TotalMicros    int64     `json:"total_micros"`
	CreatedAt      time.Time `json:"created_at"`
}

func Build(tenant string, start, end time.Time, rows []billing.RatedMessage, taxRate float64) (Invoice, error) {
	inv := Invoice{ID: fmt.Sprintf("inv-%s-%s", tenant, end.Format("20060102")), TenantID: tenant, PeriodStart: start, PeriodEnd: end, CreatedAt: time.Now()}
	grouped := map[string]Line{}
	for _, r := range rows {
		if r.TenantID != tenant || r.RatedAt.Before(start) || !r.RatedAt.Before(end) {
			continue
		}
		if inv.Currency == "" {
			inv.Currency = r.Sell.Currency
		}
		if inv.Currency != r.Sell.Currency {
			return inv, fmt.Errorf("currency mismatch")
		}
		k := r.ProviderID + ":" + r.Country
		ln := grouped[k]
		ln.Description = fmt.Sprintf("SMS %s via %s", r.Country, r.ProviderID)
		ln.Quantity += r.Segments
		if r.Segments > 0 {
			ln.UnitMicros = r.Sell.AmountMicros / int64(r.Segments)
		}
		ln.AmountMicros += r.Sell.AmountMicros
		ln.Currency = r.Sell.Currency
		grouped[k] = ln
	}
	keys := make([]string, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		ln := grouped[k]
		inv.Lines = append(inv.Lines, ln)
		inv.SubtotalMicros += ln.AmountMicros
	}
	inv.TaxMicros = int64(float64(inv.SubtotalMicros) * taxRate)
	inv.TotalMicros = inv.SubtotalMicros + inv.TaxMicros
	return inv, nil
}
func (i Invoice) CSV() string {
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	_ = w.Write([]string{"description", "quantity", "unit", "amount", "currency"})
	for _, l := range i.Lines {
		_ = w.Write([]string{l.Description, fmt.Sprint(l.Quantity), fmt.Sprintf("%.6f", float64(l.UnitMicros)/1_000_000), fmt.Sprintf("%.6f", float64(l.AmountMicros)/1_000_000), l.Currency})
	}
	_ = w.Write([]string{"subtotal", "", "", fmt.Sprintf("%.6f", float64(i.SubtotalMicros)/1_000_000), i.Currency})
	_ = w.Write([]string{"tax", "", "", fmt.Sprintf("%.6f", float64(i.TaxMicros)/1_000_000), i.Currency})
	_ = w.Write([]string{"total", "", "", fmt.Sprintf("%.6f", float64(i.TotalMicros)/1_000_000), i.Currency})
	w.Flush()
	return b.String()
}
