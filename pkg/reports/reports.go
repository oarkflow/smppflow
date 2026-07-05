package reports

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/storage"
)

type Filter struct {
	From       time.Time      `json:"from,omitempty"`
	To         time.Time      `json:"to,omitempty"`
	TenantID   string         `json:"tenant_id,omitempty"`
	ProviderID string         `json:"provider_id,omitempty"`
	Country    string         `json:"country,omitempty"`
	Sender     string         `json:"sender,omitempty"`
	CampaignID string         `json:"campaign_id,omitempty"`
	Timezone   *time.Location `json:"-"`
	GroupBy    string         `json:"group_by,omitempty"`
}
type Summary struct {
	GeneratedAt  time.Time      `json:"generated_at"`
	From         time.Time      `json:"from,omitempty"`
	To           time.Time      `json:"to,omitempty"`
	Total        int            `json:"total"`
	ByState      map[string]int `json:"by_state"`
	Parts        int            `json:"parts"`
	DLRs         int            `json:"dlrs"`
	DeliveryRate float64        `json:"delivery_rate"`
	FailureRate  float64        `json:"failure_rate"`
	PendingDLR   int            `json:"pending_dlr"`
}
type DeliveryReport struct {
	Summary Summary  `json:"summary"`
	Buckets []Bucket `json:"buckets,omitempty"`
}
type Bucket struct {
	Key       string    `json:"key"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	Submitted int       `json:"submitted"`
	Accepted  int       `json:"accepted"`
	Delivered int       `json:"delivered"`
	Failed    int       `json:"failed"`
	Expired   int       `json:"expired"`
	Rejected  int       `json:"rejected"`
	Unknown   int       `json:"unknown"`
	Parts     int       `json:"parts"`
}
type LatencyReport struct {
	Count      int                     `json:"count"`
	P50        time.Duration           `json:"p50"`
	P90        time.Duration           `json:"p90"`
	P95        time.Duration           `json:"p95"`
	P99        time.Duration           `json:"p99"`
	Average    time.Duration           `json:"average"`
	ByProvider map[string]LatencyStats `json:"by_provider,omitempty"`
}
type LatencyStats struct {
	Count   int           `json:"count"`
	P50     time.Duration `json:"p50"`
	P95     time.Duration `json:"p95"`
	P99     time.Duration `json:"p99"`
	Average time.Duration `json:"average"`
}
type ProviderReport struct {
	ProviderID             string        `json:"provider_id"`
	Submitted              int           `json:"submitted"`
	Accepted               int           `json:"accepted"`
	Delivered              int           `json:"delivered"`
	Failed                 int           `json:"failed"`
	Throttled              int           `json:"throttled"`
	Timeout                int           `json:"timeout"`
	DeliveryRate           float64       `json:"delivery_rate"`
	ErrorRate              float64       `json:"error_rate"`
	AvgSubmitLatency       time.Duration `json:"avg_submit_latency"`
	DLRLatency             LatencyStats  `json:"dlr_latency"`
	CostPerDeliveredMicros int64         `json:"cost_per_delivered_micros,omitempty"`
}
type TenantUsage struct {
	TenantID     string         `json:"tenant_id"`
	Messages     int            `json:"messages"`
	Segments     int            `json:"segments"`
	Delivered    int            `json:"delivered"`
	Failed       int            `json:"failed"`
	DeliveryRate float64        `json:"delivery_rate"`
	SpendMicros  int64          `json:"spend_micros,omitempty"`
	TopSenders   map[string]int `json:"top_senders,omitempty"`
	Countries    map[string]int `json:"countries,omitempty"`
}
type BillingReport struct {
	TenantID         string `json:"tenant_id,omitempty"`
	BillableSegments int    `json:"billable_segments"`
	CostMicros       int64  `json:"cost_micros"`
	SellMicros       int64  `json:"sell_micros"`
	MarginMicros     int64  `json:"margin_micros"`
	FailedBillable   int    `json:"failed_billable"`
	FreeMessages     int    `json:"free_messages"`
}
type ErrorReport struct {
	Top            map[string]int            `json:"top"`
	ByProvider     map[string]map[string]int `json:"by_provider"`
	Permanent      int                       `json:"permanent"`
	Retryable      int                       `json:"retryable"`
	InvalidNumber  int                       `json:"invalid_number"`
	BlockedContent int                       `json:"blocked_content"`
}
type ComplianceReport struct {
	OptOuts               int `json:"opt_outs"`
	DNDBlocks             int `json:"dnd_blocks"`
	QuietHourBlocks       int `json:"quiet_hour_blocks"`
	TemplateViolations    int `json:"template_violations"`
	SenderViolations      int `json:"sender_violations"`
	CountryRuleViolations int `json:"country_rule_violations"`
}
type OperationalReport struct {
	QueueBacklog           int `json:"queue_backlog"`
	DLQ                    int `json:"dlq"`
	WorkerThroughput       int `json:"worker_throughput"`
	ProviderDowntimeEvents int `json:"provider_downtime_events"`
	BindDisconnects        int `json:"bind_disconnects"`
	Reconnects             int `json:"reconnects"`
	EnquireFailures        int `json:"enquire_failures"`
	SLAIncidents           int `json:"sla_incidents"`
}

type WarehouseEvent struct {
	Kind       string    `json:"kind"`
	TenantID   string    `json:"tenant_id,omitempty"`
	ProviderID string    `json:"provider_id,omitempty"`
	MessageID  string    `json:"message_id,omitempty"`
	State      string    `json:"state,omitempty"`
	At         time.Time `json:"at"`
	Payload    any       `json:"payload,omitempty"`
}

func GenerateSummary(ctx context.Context, st storage.Store) (Summary, error) {
	return GenerateSummaryWithFilter(ctx, st, Filter{})
}
func GenerateSummaryWithFilter(ctx context.Context, st storage.Store, f Filter) (Summary, error) {
	msgs, err := st.ListMessages(ctx)
	if err != nil {
		return Summary{}, err
	}
	dlrs, _ := st.ListDLR(ctx)
	s := Summary{GeneratedAt: time.Now(), From: f.From, To: f.To, ByState: map[string]int{}, DLRs: len(dlrs)}
	for _, m := range msgs {
		if !match(m, f) {
			continue
		}
		s.Total++
		s.ByState[string(m.State)]++
		s.Parts += len(m.Parts)
	}
	s.PendingDLR = s.ByState[string(lifecycle.MsgSubmitted)] + s.ByState[string(lifecycle.MsgAccepted)] + s.ByState[string(lifecycle.MsgUnknown)]
	if s.Total > 0 {
		s.DeliveryRate = float64(s.ByState[string(lifecycle.MsgDelivered)]) / float64(s.Total)
		s.FailureRate = float64(s.ByState[string(lifecycle.MsgFailed)]+s.ByState[string(lifecycle.MsgUndeliverable)]+s.ByState[string(lifecycle.MsgExpired)]+s.ByState[string(lifecycle.MsgRejected)]) / float64(s.Total)
	}
	return s, nil
}
func GenerateDelivery(ctx context.Context, st storage.Store, f Filter) (DeliveryReport, error) {
	msgs, err := st.ListMessages(ctx)
	if err != nil {
		return DeliveryReport{}, err
	}
	sum, _ := GenerateSummaryWithFilter(ctx, st, f)
	rep := DeliveryReport{Summary: sum}
	if f.GroupBy == "" {
		return rep, nil
	}
	buckets := map[string]*Bucket{}
	for _, m := range msgs {
		if !match(m, f) {
			continue
		}
		key, start, end := bucket(m.CreatedAt, f)
		b := buckets[key]
		if b == nil {
			b = &Bucket{Key: key, Start: start, End: end}
			buckets[key] = b
		}
		b.Parts += len(m.Parts)
		switch m.State {
		case lifecycle.MsgSubmitted:
			b.Submitted++
		case lifecycle.MsgAccepted:
			b.Accepted++
		case lifecycle.MsgDelivered:
			b.Delivered++
		case lifecycle.MsgFailed, lifecycle.MsgUndeliverable:
			b.Failed++
		case lifecycle.MsgExpired:
			b.Expired++
		case lifecycle.MsgRejected:
			b.Rejected++
		default:
			b.Unknown++
		}
	}
	for _, b := range buckets {
		rep.Buckets = append(rep.Buckets, *b)
	}
	sort.Slice(rep.Buckets, func(i, j int) bool { return rep.Buckets[i].Start.Before(rep.Buckets[j].Start) })
	return rep, nil
}
func GenerateLatency(ctx context.Context, st storage.Store, f Filter) (LatencyReport, error) {
	msgs, err := st.ListMessages(ctx)
	if err != nil {
		return LatencyReport{}, err
	}
	all := []time.Duration{}
	by := map[string][]time.Duration{}
	for _, m := range msgs {
		if !match(m, f) {
			continue
		}
		if m.State == lifecycle.MsgDelivered && !m.CreatedAt.IsZero() && !m.UpdatedAt.IsZero() {
			d := m.UpdatedAt.Sub(m.CreatedAt)
			all = append(all, d)
			by[m.ProviderID] = append(by[m.ProviderID], d)
		}
	}
	rep := LatencyReport{ByProvider: map[string]LatencyStats{}}
	base := latStats(all)
	rep.Count = base.Count
	rep.P50 = base.P50
	rep.P90 = base.P50
	rep.P95 = base.P95
	rep.P99 = base.P99
	rep.Average = base.Average
	for k, x := range by {
		rep.ByProvider[k] = latStats(x)
	}
	return rep, nil
}
func GenerateProvider(ctx context.Context, st storage.Store, f Filter) ([]ProviderReport, error) {
	msgs, err := st.ListMessages(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]*ProviderReport{}
	for _, m := range msgs {
		if !match(m, f) {
			continue
		}
		id := m.ProviderID
		if id == "" {
			id = "unknown"
		}
		r := out[id]
		if r == nil {
			r = &ProviderReport{ProviderID: id}
			out[id] = r
		}
		r.Submitted++
		switch m.State {
		case lifecycle.MsgAccepted:
			r.Accepted++
		case lifecycle.MsgDelivered:
			r.Delivered++
		case lifecycle.MsgFailed, lifecycle.MsgUndeliverable:
			r.Failed++
		case lifecycle.MsgThrottled:
			r.Throttled++
		}
	}
	arr := []ProviderReport{}
	for _, r := range out {
		if r.Submitted > 0 {
			r.DeliveryRate = float64(r.Delivered) / float64(r.Submitted)
			r.ErrorRate = float64(r.Failed+r.Throttled) / float64(r.Submitted)
		}
		arr = append(arr, *r)
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].ProviderID < arr[j].ProviderID })
	return arr, nil
}
func GenerateTenantUsage(ctx context.Context, st storage.Store, f Filter) ([]TenantUsage, error) {
	msgs, err := st.ListMessages(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]*TenantUsage{}
	for _, m := range msgs {
		if !match(m, f) {
			continue
		}
		id := m.TenantID
		if id == "" {
			id = "unknown"
		}
		u := out[id]
		if u == nil {
			u = &TenantUsage{TenantID: id, TopSenders: map[string]int{}, Countries: map[string]int{}}
			out[id] = u
		}
		u.Messages++
		u.Segments += max(1, len(m.Parts))
		u.TopSenders[m.From.Addr]++
		if c := m.Meta["country"]; c != "" {
			u.Countries[c]++
		}
		if m.State == lifecycle.MsgDelivered {
			u.Delivered++
		}
		if m.State == lifecycle.MsgFailed || m.State == lifecycle.MsgUndeliverable {
			u.Failed++
		}
	}
	arr := []TenantUsage{}
	for _, u := range out {
		if u.Messages > 0 {
			u.DeliveryRate = float64(u.Delivered) / float64(u.Messages)
		}
		arr = append(arr, *u)
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].TenantID < arr[j].TenantID })
	return arr, nil
}
func GenerateErrors(ctx context.Context, st storage.Store, f Filter) (ErrorReport, error) {
	msgs, err := st.ListMessages(ctx)
	if err != nil {
		return ErrorReport{}, err
	}
	rep := ErrorReport{Top: map[string]int{}, ByProvider: map[string]map[string]int{}}
	for _, m := range msgs {
		if !match(m, f) {
			continue
		}
		for _, p := range m.Parts {
			code := p.ErrorCode
			if code == "" && p.LastError != "" {
				code = p.LastError
			}
			if code == "" {
				continue
			}
			rep.Top[code]++
			if rep.ByProvider[m.ProviderID] == nil {
				rep.ByProvider[m.ProviderID] = map[string]int{}
			}
			rep.ByProvider[m.ProviderID][code]++
		}
	}
	return rep, nil
}
func WriteJSON(ctx context.Context, w io.Writer, st storage.Store) error {
	s, err := GenerateSummary(ctx, st)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(s)
}
func WriteCSV(ctx context.Context, w io.Writer, st storage.Store) error {
	msgs, err := st.ListMessages(ctx)
	if err != nil {
		return err
	}
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{"id", "tenant", "provider", "campaign", "from", "to", "state", "provider_message_id", "parts", "created_at", "updated_at"})
	for _, m := range msgs {
		_ = cw.Write([]string{string(m.ID), m.TenantID, m.ProviderID, m.CampaignID, m.From.Addr, m.To.Addr, string(m.State), m.ProviderMessageID, itoa(len(m.Parts)), m.CreatedAt.Format(time.RFC3339Nano), m.UpdatedAt.Format(time.RFC3339Nano)})
	}
	cw.Flush()
	return cw.Error()
}
func WriteNDJSON(ctx context.Context, w io.Writer, st storage.Store, f Filter) error {
	msgs, err := st.ListMessages(ctx)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	for _, m := range msgs {
		if match(m, f) {
			if err := enc.Encode(WarehouseEvent{Kind: "message", TenantID: m.TenantID, ProviderID: m.ProviderID, MessageID: string(m.ID), State: string(m.State), At: m.UpdatedAt, Payload: m}); err != nil {
				return err
			}
		}
	}
	dlrs, _ := st.ListDLR(ctx)
	for _, d := range dlrs {
		if err := enc.Encode(WarehouseEvent{Kind: "dlr", ProviderID: d.ProviderID, MessageID: d.MessageID, State: string(d.State), At: d.ReceivedAt, Payload: d}); err != nil {
			return err
		}
	}
	return nil
}
func match(m *message.Message, f Filter) bool {
	if f.TenantID != "" && m.TenantID != f.TenantID {
		return false
	}
	if f.ProviderID != "" && m.ProviderID != f.ProviderID {
		return false
	}
	if f.Sender != "" && m.From.Addr != f.Sender {
		return false
	}
	if f.CampaignID != "" && m.CampaignID != f.CampaignID {
		return false
	}
	if !f.From.IsZero() && m.CreatedAt.Before(f.From) {
		return false
	}
	if !f.To.IsZero() && m.CreatedAt.After(f.To) {
		return false
	}
	return true
}
func bucket(t time.Time, f Filter) (string, time.Time, time.Time) {
	loc := f.Timezone
	if loc == nil {
		loc = time.UTC
	}
	tt := t.In(loc)
	switch f.GroupBy {
	case "minute":
		s := tt.Truncate(time.Minute)
		return s.Format(time.RFC3339), s, s.Add(time.Minute)
	case "hour":
		s := tt.Truncate(time.Hour)
		return s.Format("2006-01-02T15:00"), s, s.Add(time.Hour)
	case "month":
		s := time.Date(tt.Year(), tt.Month(), 1, 0, 0, 0, 0, loc)
		return s.Format("2006-01"), s, s.AddDate(0, 1, 0)
	default:
		s := time.Date(tt.Year(), tt.Month(), tt.Day(), 0, 0, 0, 0, loc)
		return s.Format("2006-01-02"), s, s.AddDate(0, 0, 1)
	}
}
func latStats(xs []time.Duration) LatencyStats {
	if len(xs) == 0 {
		return LatencyStats{}
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	var sum time.Duration
	for _, x := range xs {
		sum += x
	}
	return LatencyStats{Count: len(xs), P50: percentile(xs, 0.50), P95: percentile(xs, 0.95), P99: percentile(xs, 0.99), Average: time.Duration(int64(sum) / int64(len(xs)))}
}
func percentile(xs []time.Duration, p float64) time.Duration {
	if len(xs) == 0 {
		return 0
	}
	idx := int(float64(len(xs)-1) * p)
	return xs[idx]
}
func itoa(n int) string { return fmt.Sprint(n) }
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
