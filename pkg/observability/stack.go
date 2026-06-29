package observability

import (
	"encoding/json"
	"io"
)

type AlertRule struct {
	Name     string `json:"name"`
	Expr     string `json:"expr"`
	For      string `json:"for"`
	Severity string `json:"severity"`
	Summary  string `json:"summary"`
}
type Dashboard struct {
	Title  string  `json:"title"`
	Panels []Panel `json:"panels"`
}
type Panel struct {
	Title string `json:"title"`
	Query string `json:"query"`
	Unit  string `json:"unit"`
}
type StackConfig struct {
	ServiceName   string      `json:"service_name"`
	PrometheusJob string      `json:"prometheus_job"`
	OTLPPort      int         `json:"otlp_port"`
	MetricsPort   int         `json:"metrics_port"`
	Alerts        []AlertRule `json:"alerts"`
	Dashboard     Dashboard   `json:"dashboard"`
}

func DefaultStack() StackConfig {
	return StackConfig{ServiceName: "smppflow", PrometheusJob: "smppflow", OTLPPort: 4317, MetricsPort: 9090, Alerts: []AlertRule{{Name: "ProviderHighErrorRate", Expr: "rate(smpp_provider_errors_total[5m]) > 0.05", For: "10m", Severity: "critical", Summary: "Provider error rate is high"}, {Name: "DLRLatencyHigh", Expr: "histogram_quantile(0.95, rate(smpp_dlr_latency_seconds_bucket[5m])) > 300", For: "15m", Severity: "warning", Summary: "DLR p95 latency is high"}, {Name: "QueueBacklog", Expr: "smpp_queue_depth > 100000", For: "5m", Severity: "critical", Summary: "Queue backlog is growing"}}, Dashboard: Dashboard{Title: "SMPPFlow Carrier Gateway", Panels: []Panel{{Title: "Submit TPS", Query: "sum(rate(smpp_submit_total[1m]))", Unit: "ops"}, {Title: "Provider errors", Query: "sum by(provider)(rate(smpp_provider_errors_total[5m]))", Unit: "percent"}, {Title: "DLR latency p95", Query: "histogram_quantile(0.95, rate(smpp_dlr_latency_seconds_bucket[5m]))", Unit: "seconds"}, {Title: "Queue depth", Query: "smpp_queue_depth", Unit: "messages"}}}}
}
func WriteJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
