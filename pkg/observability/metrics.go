// Package observability provides a small, dependency-free metrics store with
// a Prometheus text-exposition-format exporter, plus a lightweight tracing
// primitive built only on the standard library (see trace.go).
//
// The metrics store is designed to be hit from many goroutines per inbound
// SMPP message (bind/submit/deliver paths), so the hot paths (Inc/Gauge/
// Observe) avoid taking a single coarse lock for every call. Each metric
// series (a metric name + its label set) is stored behind its own atomic
// value once created; a sync.Map is used for the get-or-create step so
// concurrent readers/writers of *different* series never contend, and
// readers of an *existing* series never block writers of that same series
// (updates are done with lock-free CAS loops on the bit pattern of the
// float64 value).
package observability

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultBuckets are the histogram bucket upper bounds (in seconds) used
// when a metric name has not been given explicit buckets via SetBuckets.
// These mirror the common Prometheus client-library defaults, which are a
// reasonable fit for request/message latencies.
var DefaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

type metricKind int

const (
	kindCounter metricKind = iota
	kindGauge
	kindHistogram
)

func (k metricKind) String() string {
	switch k {
	case kindCounter:
		return "counter"
	case kindGauge:
		return "gauge"
	case kindHistogram:
		return "histogram"
	default:
		return "untyped"
	}
}

type labelPair struct {
	Key, Value string
}

type seriesMeta struct {
	Name   string
	Labels []labelPair
}

// labelString renders the metadata's labels in Prometheus exposition
// format, escaping backslashes, double quotes and newlines in values per
// the spec: https://prometheus.io/docs/instrumenting/exposition_formats/
func (s seriesMeta) labelString() string {
	if len(s.Labels) == 0 {
		return ""
	}
	parts := make([]string, len(s.Labels))
	for i, lp := range s.Labels {
		parts[i] = lp.Key + "=\"" + escapeLabelValue(lp.Value) + "\""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escapeLabelValue(v string) string {
	if !strings.ContainsAny(v, "\\\"\n") {
		return v
	}
	var b strings.Builder
	b.Grow(len(v) + 4)
	for _, r := range v {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// counterEntry / gaugeEntry hold a single series' value as the bit pattern
// of a float64 inside an atomic.Uint64, so increments/reads never need a
// mutex.
type counterEntry struct {
	seriesMeta
	bits atomic.Uint64
}

type gaugeEntry struct {
	seriesMeta
	bits atomic.Uint64
}

// histEntry holds a single series' histogram state. buckets are the
// (ascending, deduplicated) finite upper bounds; counts holds one atomic
// counter per bucket plus a trailing +Inf bucket (len(buckets)+1 total).
// Each observation increments exactly one bucket slot (the first bucket
// whose bound is >= the observed value, or the +Inf slot); cumulative
// counts are computed on read, which keeps writes O(log n) (binary search)
// and allocation-free.
type histEntry struct {
	seriesMeta
	buckets []float64
	counts  []atomic.Uint64
	count   atomic.Uint64
	sumBits atomic.Uint64
}

func newHistEntry(meta seriesMeta, buckets []float64) *histEntry {
	h := &histEntry{seriesMeta: meta, buckets: buckets, counts: make([]atomic.Uint64, len(buckets)+1)}
	return h
}

func (h *histEntry) observe(v float64) {
	idx := sort.SearchFloat64s(h.buckets, v)
	h.counts[idx].Add(1)
	h.count.Add(1)
	addFloatBits(&h.sumBits, v)
}

// cumulativeCounts returns the cumulative bucket counts (index i = count of
// observations with value <= buckets[i]) plus the total count (the +Inf
// bucket).
func (h *histEntry) cumulativeCounts() ([]uint64, uint64) {
	out := make([]uint64, len(h.buckets))
	var running uint64
	for i := range h.buckets {
		running += h.counts[i].Load()
		out[i] = running
	}
	running += h.counts[len(h.buckets)].Load()
	return out, running
}

func addFloatBits(addr *atomic.Uint64, delta float64) {
	for {
		old := addr.Load()
		newV := math.Float64frombits(old) + delta
		if addr.CompareAndSwap(old, math.Float64bits(newV)) {
			return
		}
	}
}

// Metrics is a concurrency-safe, dependency-free store for counters,
// gauges and histograms, with a Prometheus text-exposition-format
// exporter.
type Metrics struct {
	counters sync.Map // series key -> *counterEntry
	gauges   sync.Map // series key -> *gaugeEntry
	hists    sync.Map // series key -> *histEntry

	help    sync.Map // metric name -> help string
	buckets sync.Map // metric name -> []float64 (custom buckets)
}

func New() *Metrics {
	return &Metrics{}
}

// SetHelp sets the HELP text emitted for a metric name. Optional; if never
// called a generic help string is generated.
func (m *Metrics) SetHelp(name, help string) {
	m.help.Store(name, help)
}

// SetBuckets configures the histogram bucket upper bounds used for a
// metric name. It must be called before the first Observe for that name to
// take effect for all of that metric's label combinations; buckets are
// fixed per-series at creation time.
func (m *Metrics) SetBuckets(name string, buckets []float64) {
	b := append([]float64(nil), buckets...)
	sort.Float64s(b)
	m.buckets.Store(name, b)
}

func sortedLabels(labels map[string]string) []labelPair {
	if len(labels) == 0 {
		return nil
	}
	out := make([]labelPair, 0, len(labels))
	for k, v := range labels {
		out = append(out, labelPair{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func seriesKey(name string, lp []labelPair) string {
	if len(lp) == 0 {
		return name
	}
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, p := range lp {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(p.Key)
		b.WriteByte('=')
		b.WriteString(p.Value)
	}
	b.WriteByte('}')
	return b.String()
}

// Inc increments a counter by v (or by 1 if v == 0). Safe for concurrent
// use; never blocks on other metric names or series.
func (m *Metrics) Inc(name string, labels map[string]string, v float64) {
	if v == 0 {
		v = 1
	}
	lp := sortedLabels(labels)
	key := seriesKey(name, lp)
	e, ok := m.counters.Load(key)
	if !ok {
		e, _ = m.counters.LoadOrStore(key, &counterEntry{seriesMeta: seriesMeta{Name: name, Labels: lp}})
	}
	addFloatBits(&e.(*counterEntry).bits, v)
}

// Gauge sets a gauge to v. Safe for concurrent use.
func (m *Metrics) Gauge(name string, labels map[string]string, v float64) {
	lp := sortedLabels(labels)
	key := seriesKey(name, lp)
	e, ok := m.gauges.Load(key)
	if !ok {
		e, _ = m.gauges.LoadOrStore(key, &gaugeEntry{seriesMeta: seriesMeta{Name: name, Labels: lp}})
	}
	e.(*gaugeEntry).bits.Store(math.Float64bits(v))
}

// AddGauge adds delta to a gauge's current value (creating it at delta if
// absent). Safe for concurrent use.
func (m *Metrics) AddGauge(name string, labels map[string]string, delta float64) {
	lp := sortedLabels(labels)
	key := seriesKey(name, lp)
	e, ok := m.gauges.Load(key)
	if !ok {
		e, _ = m.gauges.LoadOrStore(key, &gaugeEntry{seriesMeta: seriesMeta{Name: name, Labels: lp}})
	}
	addFloatBits(&e.(*gaugeEntry).bits, delta)
}

// Observe records a duration (in seconds) into a histogram. Safe for
// concurrent use; never blocks on other metric names or series.
func (m *Metrics) Observe(name string, labels map[string]string, d time.Duration) {
	lp := sortedLabels(labels)
	key := seriesKey(name, lp)
	e, ok := m.hists.Load(key)
	if !ok {
		buckets := DefaultBuckets
		if custom, ok := m.buckets.Load(name); ok {
			buckets = custom.([]float64)
		}
		e, _ = m.hists.LoadOrStore(key, newHistEntry(seriesMeta{Name: name, Labels: lp}, buckets))
	}
	e.(*histEntry).observe(d.Seconds())
}

// Quantile returns an approximate quantile (0 <= q <= 1) for a histogram
// series, using linear interpolation within the bucket that contains the
// target rank (the same method Prometheus' histogram_quantile() uses). The
// second return value is false if the series has no observations.
func (m *Metrics) Quantile(name string, labels map[string]string, q float64) (float64, bool) {
	lp := sortedLabels(labels)
	key := seriesKey(name, lp)
	v, ok := m.hists.Load(key)
	if !ok {
		return 0, false
	}
	h := v.(*histEntry)
	cum, total := h.cumulativeCounts()
	if total == 0 {
		return 0, false
	}
	if q <= 0 {
		return 0, true
	}
	if q >= 1 {
		// Highest observed bucket; if it's the +Inf overflow bucket we
		// cannot bound it above, so report +Inf.
		if len(cum) == 0 || cum[len(cum)-1] < total {
			return math.Inf(1), true
		}
		return h.buckets[len(h.buckets)-1], true
	}
	target := q * float64(total)
	prevBoundary := 0.0
	var prevCount uint64
	for i, boundary := range h.buckets {
		if float64(cum[i]) >= target {
			bucketCount := cum[i] - prevCount
			if bucketCount == 0 {
				return boundary, true
			}
			frac := (target - float64(prevCount)) / float64(bucketCount)
			return prevBoundary + frac*(boundary-prevBoundary), true
		}
		prevBoundary = boundary
		prevCount = cum[i]
	}
	// Falls into the +Inf bucket: cannot interpolate an upper bound.
	return math.Inf(1), true
}

// Prometheus renders all metrics in Prometheus text exposition format
// (https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format),
// including correct HELP/TYPE stanzas, escaped label values, and
// cumulative histogram buckets terminated by a +Inf bucket.
func (m *Metrics) Prometheus() string {
	var b strings.Builder
	m.writeCounters(&b)
	m.writeGauges(&b)
	m.writeHistograms(&b)
	return b.String()
}

func (m *Metrics) helpFor(name, kind string) string {
	if h, ok := m.help.Load(name); ok {
		return h.(string)
	}
	return fmt.Sprintf("%s %s metric", name, kind)
}

func (m *Metrics) writeCounters(b *strings.Builder) {
	entries := collect[*counterEntry](&m.counters)
	byName := groupByName(entries, func(e *counterEntry) seriesMeta { return e.seriesMeta })
	for _, name := range sortedKeys(byName) {
		fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s counter\n", name, m.helpFor(name, "counter"), name)
		group := byName[name]
		sort.Slice(group, func(i, j int) bool { return group[i].seriesMeta.labelString() < group[j].seriesMeta.labelString() })
		for _, e := range group {
			v := math.Float64frombits(e.bits.Load())
			fmt.Fprintf(b, "%s%s %s\n", name, e.labelString(), formatFloat(v))
		}
	}
}

func (m *Metrics) writeGauges(b *strings.Builder) {
	entries := collect[*gaugeEntry](&m.gauges)
	byName := groupByName(entries, func(e *gaugeEntry) seriesMeta { return e.seriesMeta })
	for _, name := range sortedKeys(byName) {
		fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s gauge\n", name, m.helpFor(name, "gauge"), name)
		group := byName[name]
		sort.Slice(group, func(i, j int) bool { return group[i].seriesMeta.labelString() < group[j].seriesMeta.labelString() })
		for _, e := range group {
			v := math.Float64frombits(e.bits.Load())
			fmt.Fprintf(b, "%s%s %s\n", name, e.labelString(), formatFloat(v))
		}
	}
}

func (m *Metrics) writeHistograms(b *strings.Builder) {
	entries := collect[*histEntry](&m.hists)
	byName := groupByName(entries, func(e *histEntry) seriesMeta { return e.seriesMeta })
	for _, name := range sortedKeys(byName) {
		fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s histogram\n", name, m.helpFor(name, "histogram"), name)
		group := byName[name]
		sort.Slice(group, func(i, j int) bool { return group[i].seriesMeta.labelString() < group[j].seriesMeta.labelString() })
		for _, e := range group {
			base := e.seriesMeta.labelString()
			cum, total := e.cumulativeCounts()
			for i, boundary := range e.buckets {
				fmt.Fprintf(b, "%s_bucket%s %d\n", name, withLE(base, formatFloat(boundary)), cum[i])
			}
			fmt.Fprintf(b, "%s_bucket%s %d\n", name, withLE(base, "+Inf"), total)
			sum := math.Float64frombits(e.sumBits.Load())
			fmt.Fprintf(b, "%s_sum%s %s\n", name, base, formatFloat(sum))
			fmt.Fprintf(b, "%s_count%s %d\n", name, base, e.count.Load())
		}
	}
}

// withLE inserts a le="..." label into an existing rendered label string
// (which may be empty, e.g. "" or "{foo=\"bar\"}").
func withLE(base, le string) string {
	leLabel := `le="` + le + `"`
	if base == "" {
		return "{" + leLabel + "}"
	}
	return base[:len(base)-1] + "," + leLabel + "}"
}

func formatFloat(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	if math.IsInf(v, -1) {
		return "-Inf"
	}
	if math.IsNaN(v) {
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func collect[T any](sm *sync.Map) []T {
	var out []T
	sm.Range(func(_, value any) bool {
		out = append(out, value.(T))
		return true
	})
	return out
}

func groupByName[T any](entries []T, meta func(T) seriesMeta) map[string][]T {
	out := map[string][]T{}
	for _, e := range entries {
		name := meta(e).Name
		out[name] = append(out[name], e)
	}
	return out
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
