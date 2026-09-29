package observability

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCounterBasic(t *testing.T) {
	m := New()
	m.Inc("smpp_submit_total", map[string]string{"provider": "p1"}, 0)
	m.Inc("smpp_submit_total", map[string]string{"provider": "p1"}, 2)
	m.Inc("smpp_submit_total", map[string]string{"provider": "p2"}, 5)

	out := m.Prometheus()
	if !strings.Contains(out, "# HELP smpp_submit_total") {
		t.Fatalf("missing HELP line:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE smpp_submit_total counter") {
		t.Fatalf("missing TYPE line:\n%s", out)
	}
	if !strings.Contains(out, `smpp_submit_total{provider="p1"} 3`) {
		t.Fatalf("expected p1 counter at 3, got:\n%s", out)
	}
	if !strings.Contains(out, `smpp_submit_total{provider="p2"} 5`) {
		t.Fatalf("expected p2 counter at 5, got:\n%s", out)
	}
}

func TestGaugeSetAndAdd(t *testing.T) {
	m := New()
	m.Gauge("smpp_queue_depth", nil, 10)
	m.AddGauge("smpp_queue_depth", nil, 5)
	m.AddGauge("smpp_queue_depth", nil, -3)

	out := m.Prometheus()
	if !strings.Contains(out, "# TYPE smpp_queue_depth gauge") {
		t.Fatalf("missing TYPE line:\n%s", out)
	}
	if !strings.Contains(out, "smpp_queue_depth 12") {
		t.Fatalf("expected gauge at 12, got:\n%s", out)
	}
}

func TestLabelEscaping(t *testing.T) {
	m := New()
	// Value contains a backslash, a double quote and a newline: all three
	// must be escaped per the Prometheus exposition format.
	m.Inc("smpp_provider_errors_total", map[string]string{"reason": "bad \"quote\" and \\slash\\ and\nline"}, 1)
	out := m.Prometheus()

	want := `reason="bad \"quote\" and \\slash\\ and\nline"`
	if !strings.Contains(out, want) {
		t.Fatalf("expected escaped label %q in output:\n%s", want, out)
	}
}

func TestLabelEscapingNoLiteralNewline(t *testing.T) {
	m := New()
	m.Inc("x", map[string]string{"k": "a\nb"}, 1)
	out := m.Prometheus()
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	// Exactly: HELP, TYPE, and one metric line. A literal newline in the
	// label value would produce an extra line.
	var metricLines int
	for _, l := range lines {
		if strings.HasPrefix(l, "x{") {
			metricLines++
			if !strings.Contains(l, `k="a\nb"`) {
				t.Fatalf("expected escaped \\n in line: %q", l)
			}
		}
	}
	if metricLines != 1 {
		t.Fatalf("expected exactly 1 metric line (newline must be escaped, not literal), got %d, full output:\n%s", metricLines, out)
	}
}

func TestHistogramBucketsCumulativeAndInf(t *testing.T) {
	m := New()
	m.SetBuckets("smpp_dlr_latency_seconds", []float64{0.1, 0.5, 1})
	observations := []float64{0.05, 0.2, 0.2, 0.6, 5.0}
	for _, o := range observations {
		m.Observe("smpp_dlr_latency_seconds", nil, time.Duration(o*float64(time.Second)))
	}
	out := m.Prometheus()

	le := map[string]int{}
	re := regexp.MustCompile(`smpp_dlr_latency_seconds_bucket\{le="([^"]+)"\} (\d+)`)
	for _, match := range re.FindAllStringSubmatch(out, -1) {
		var n int
		fmt.Sscanf(match[2], "%d", &n)
		le[match[1]] = n
	}
	// value 0.05 <= 0.1: bucket 0.1 count 1
	if le["0.1"] != 1 {
		t.Fatalf("bucket le=0.1 expected 1, got %d (%v)", le["0.1"], le)
	}
	// 0.05,0.2,0.2 <= 0.5: cumulative 3
	if le["0.5"] != 3 {
		t.Fatalf("bucket le=0.5 expected 3 (cumulative), got %d (%v)", le["0.5"], le)
	}
	// 0.05,0.2,0.2,0.6 <= 1: cumulative 4
	if le["1"] != 4 {
		t.Fatalf("bucket le=1 expected 4 (cumulative), got %d (%v)", le["1"], le)
	}
	// +Inf must equal total count (5)
	if le["+Inf"] != 5 {
		t.Fatalf("bucket le=+Inf expected 5 (total), got %d (%v)", le["+Inf"], le)
	}
	if !strings.Contains(out, "smpp_dlr_latency_seconds_count 5") {
		t.Fatalf("expected _count 5 line:\n%s", out)
	}
	wantSum := 0.05 + 0.2 + 0.2 + 0.6 + 5.0
	if !strings.Contains(out, fmt.Sprintf("smpp_dlr_latency_seconds_sum %s", formatFloat(wantSum))) {
		t.Fatalf("expected _sum %v line:\n%s", wantSum, out)
	}
}

func TestHistogramBoundaryInclusive(t *testing.T) {
	// A value exactly equal to a bucket boundary must fall into that
	// bucket (le comparison is "<="), not the next one — a classic
	// off-by-one bug.
	m := New()
	m.SetBuckets("h", []float64{1, 2})
	m.Observe("h", nil, time.Second) // exactly 1.0s
	out := m.Prometheus()
	if !strings.Contains(out, `h_bucket{le="1"} 1`) {
		t.Fatalf("expected value==boundary to land in that bucket (le inclusive):\n%s", out)
	}
	if !strings.Contains(out, `h_bucket{le="2"} 1`) {
		t.Fatalf("expected cumulative count in next bucket:\n%s", out)
	}
}

func TestQuantile(t *testing.T) {
	m := New()
	m.SetBuckets("q", []float64{0.1, 0.2, 0.3, 0.4, 0.5})
	// 10 observations spread evenly across buckets so p50 should land
	// around the middle bucket.
	vals := []float64{0.05, 0.05, 0.15, 0.15, 0.25, 0.25, 0.35, 0.35, 0.45, 0.45}
	for _, v := range vals {
		m.Observe("q", nil, time.Duration(v*float64(time.Second)))
	}
	p50, ok := m.Quantile("q", nil, 0.5)
	if !ok {
		t.Fatal("expected ok")
	}
	if p50 < 0.2 || p50 > 0.35 {
		t.Fatalf("expected p50 roughly in [0.2,0.35], got %v", p50)
	}
	if _, ok := m.Quantile("missing", nil, 0.5); ok {
		t.Fatal("expected not-ok for missing series")
	}
}

func TestConcurrentAccess(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	const goroutines = 50
	const perGoroutine = 200
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			labels := map[string]string{"worker": fmt.Sprintf("%d", i%5)}
			for j := 0; j < perGoroutine; j++ {
				m.Inc("concurrent_counter", labels, 1)
				m.Gauge("concurrent_gauge", labels, float64(j))
				m.Observe("concurrent_hist", labels, time.Millisecond)
			}
		}(i)
	}
	wg.Wait()
	out := m.Prometheus()
	total := 0.0
	re := regexp.MustCompile(`concurrent_counter\{worker="\d"\} (\d+)`)
	for _, match := range re.FindAllStringSubmatch(out, -1) {
		var n float64
		fmt.Sscanf(match[1], "%f", &n)
		total += n
	}
	if total != float64(goroutines*perGoroutine) {
		t.Fatalf("expected total %d, got %v\n%s", goroutines*perGoroutine, total, out)
	}
}

func TestFormatFloatSpecialValues(t *testing.T) {
	if formatFloat(math.Inf(1)) != "+Inf" {
		t.Fatal("expected +Inf")
	}
	if formatFloat(math.Inf(-1)) != "-Inf" {
		t.Fatal("expected -Inf")
	}
	if formatFloat(math.NaN()) != "NaN" {
		t.Fatal("expected NaN")
	}
}

func TestHelpOverride(t *testing.T) {
	m := New()
	m.SetHelp("my_metric", "custom help text")
	m.Inc("my_metric", nil, 1)
	out := m.Prometheus()
	if !strings.Contains(out, "# HELP my_metric custom help text\n") {
		t.Fatalf("expected custom help text:\n%s", out)
	}
}
