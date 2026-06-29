package observability

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type Metrics struct {
	mu       sync.RWMutex
	counters map[string]float64
	gauges   map[string]float64
	hist     map[string][]float64
}

func New() *Metrics {
	return &Metrics{counters: map[string]float64{}, gauges: map[string]float64{}, hist: map[string][]float64{}}
}
func (m *Metrics) Inc(name string, labels map[string]string, v float64) {
	if v == 0 {
		v = 1
	}
	m.mu.Lock()
	m.counters[key(name, labels)] += v
	m.mu.Unlock()
}
func (m *Metrics) Gauge(name string, labels map[string]string, v float64) {
	m.mu.Lock()
	m.gauges[key(name, labels)] = v
	m.mu.Unlock()
}
func (m *Metrics) Observe(name string, labels map[string]string, d time.Duration) {
	m.mu.Lock()
	m.hist[key(name, labels)] = append(m.hist[key(name, labels)], d.Seconds())
	m.mu.Unlock()
}
func (m *Metrics) Prometheus() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var b strings.Builder
	keys := make([]string, 0, len(m.counters))
	for k := range m.counters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s %g\n", k, m.counters[k])
	}
	keys = keys[:0]
	for k := range m.gauges {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s %g\n", k, m.gauges[k])
	}
	for k, xs := range m.hist {
		if len(xs) == 0 {
			continue
		}
		sum := 0.0
		for _, x := range xs {
			sum += x
		}
		fmt.Fprintf(&b, "%s_count %d\n%s_sum %g\n", k, len(xs), k, sum)
	}
	return b.String()
}
func key(n string, l map[string]string) string {
	if len(l) == 0 {
		return n
	}
	ks := make([]string, 0, len(l))
	for k := range l {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	parts := make([]string, 0, len(ks))
	for _, k := range ks {
		parts = append(parts, fmt.Sprintf("%s=\"%s\"", k, strings.ReplaceAll(l[k], "\"", "'")))
	}
	return n + "{" + strings.Join(parts, ",") + "}"
}
