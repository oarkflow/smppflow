package certification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/message"
	"github.com/oarkflow/smppflow/protocol"
	"github.com/oarkflow/smppflow/provider"
)

type ThrottleBehavior struct {
	TemporaryStatuses []protocol.CommandStatus `json:"temporary_statuses"`
	BackoffMin        time.Duration            `json:"backoff_min"`
	BackoffMax        time.Duration            `json:"backoff_max"`
	WindowPenalty     float64                  `json:"window_penalty"`
	TPSPenalty        float64                  `json:"tps_penalty"`
}

type TONNPIRule struct {
	SourcePattern string       `json:"source_pattern"`
	SourceTON     protocol.TON `json:"source_ton"`
	SourceNPI     protocol.NPI `json:"source_npi"`
	DestPattern   string       `json:"dest_pattern"`
	DestTON       protocol.TON `json:"dest_ton"`
	DestNPI       protocol.NPI `json:"dest_npi"`
}
type TLVQuirk struct {
	Tag       protocol.TLVTag `json:"tag"`
	Name      string          `json:"name"`
	Required  bool            `json:"required"`
	Forbidden bool            `json:"forbidden"`
	MinLen    int             `json:"min_len"`
	MaxLen    int             `json:"max_len"`
	Notes     string          `json:"notes"`
}
type DLRSample struct {
	Name              string `json:"name"`
	Raw               string `json:"raw"`
	MessageID         string `json:"message_id"`
	ProviderMessageID string `json:"provider_message_id"`
	ExpectedState     string `json:"expected_state"`
	ErrorCode         string `json:"error_code"`
}
type AcceptanceCase struct {
	Name         string                 `json:"name"`
	Operation    protocol.CommandID     `json:"operation"`
	Version      protocol.Version       `json:"version"`
	Source       string                 `json:"source"`
	Destination  string                 `json:"destination"`
	Text         string                 `json:"text"`
	RequireTLV   []protocol.TLVTag      `json:"require_tlv"`
	ForbidTLV    []protocol.TLVTag      `json:"forbid_tlv"`
	ExpectStatus protocol.CommandStatus `json:"expect_status"`
}

type Pack struct {
	ID              string                 `json:"id"`
	Name            string                 `json:"name"`
	ProviderID      string                 `json:"provider_id"`
	Version         protocol.Version       `json:"version"`
	BindMode        protocol.BindMode      `json:"bind_mode"`
	SystemType      string                 `json:"system_type"`
	WindowSize      int                    `json:"window_size"`
	TPS             int                    `json:"tps"`
	EnquireInterval time.Duration          `json:"enquire_interval"`
	ResponseTimeout time.Duration          `json:"response_timeout"`
	Multipart       provider.MultipartMode `json:"multipart"`
	DLR             provider.DLRFormat     `json:"dlr"`
	TONNPIRules     []TONNPIRule           `json:"ton_npi_rules"`
	TLVQuirks       []TLVQuirk             `json:"tlv_quirks"`
	DLRSamples      []DLRSample            `json:"dlr_samples"`
	Acceptance      []AcceptanceCase       `json:"acceptance"`
	Throttle        ThrottleBehavior       `json:"throttle"`
	Meta            map[string]string      `json:"meta"`
}

func GenericPack(id string) Pack {
	return Pack{ID: id, Name: id, ProviderID: id, Version: protocol.Version34, BindMode: protocol.BindModeTransceiver, WindowSize: 16, TPS: 100, EnquireInterval: 30 * time.Second, ResponseTimeout: 10 * time.Second, Multipart: provider.MultipartUDH, DLR: provider.DLRMixed, Throttle: ThrottleBehavior{TemporaryStatuses: []protocol.CommandStatus{protocol.ESME_RTHROTTLED, protocol.ESME_RMSGQFUL, protocol.ESME_RSYSERR}, BackoffMin: time.Second, BackoffMax: time.Minute, WindowPenalty: .5, TPSPenalty: .75}, Acceptance: []AcceptanceCase{{Name: "submit_sm basic", Operation: protocol.SubmitSM, Version: protocol.Version34, Source: "ACME", Destination: "+16502530000", Text: "hello", ExpectStatus: protocol.ESME_ROK}}}
}

func (p Pack) Apply(base provider.Profile) provider.Profile {
	if base.ID == "" {
		base.ID = p.ProviderID
	}
	if p.Version != 0 {
		base.Version = p.Version
	}
	if p.BindMode != "" {
		base.BindMode = p.BindMode
	}
	if p.SystemType != "" {
		base.SystemType = p.SystemType
	}
	if p.WindowSize > 0 {
		base.WindowSize = p.WindowSize
	}
	if p.TPS > 0 {
		base.TPS = p.TPS
	}
	if p.EnquireInterval > 0 {
		base.EnquireInterval = p.EnquireInterval
	}
	if p.ResponseTimeout > 0 {
		base.ResponseTimeout = p.ResponseTimeout
	}
	if p.Multipart != "" {
		base.Multipart = p.Multipart
	}
	if p.DLR != "" {
		base.DLR = p.DLR
	}
	return base
}

type Registry struct {
	mu    sync.RWMutex
	packs map[string]Pack
}

func NewRegistry() *Registry { return &Registry{packs: map[string]Pack{}} }
func (r *Registry) Register(p Pack) error {
	if p.ID == "" {
		return errors.New("certification pack id is required")
	}
	if p.ProviderID == "" {
		p.ProviderID = p.ID
	}
	r.mu.Lock()
	r.packs[p.ID] = p
	r.mu.Unlock()
	return nil
}
func (r *Registry) Get(id string) (Pack, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.packs[id]
	return p, ok
}
func (r *Registry) List() []Pack {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Pack, 0, len(r.packs))
	for _, p := range r.packs {
		out = append(out, p)
	}
	return out
}
func LoadJSON(rd io.Reader) (Pack, error) {
	var p Pack
	err := json.NewDecoder(rd).Decode(&p)
	return p, err
}
func SaveJSON(w io.Writer, p Pack) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(p)
}

type Finding struct {
	Case     string
	Severity string
	Message  string
}
type Report struct {
	ProviderID string
	Passed     bool
	Findings   []Finding
}

func ValidateSubmit(ctx context.Context, pack Pack, req message.SubmitRequest, tlvs []protocol.TLV) Report {
	_ = ctx
	rep := Report{ProviderID: pack.ProviderID, Passed: true}
	for _, q := range pack.TLVQuirks {
		present := false
		var l int
		for _, t := range tlvs {
			if t.Tag == q.Tag {
				present = true
				l = len(t.Value)
				break
			}
		}
		if q.Required && !present {
			rep.add("tlv", "error", fmt.Sprintf("required TLV %s missing", q.Tag))
		}
		if q.Forbidden && present {
			rep.add("tlv", "error", fmt.Sprintf("forbidden TLV %s present", q.Tag))
		}
		if present && q.MinLen > 0 && l < q.MinLen {
			rep.add("tlv", "error", fmt.Sprintf("TLV %s shorter than %d", q.Tag, q.MinLen))
		}
		if present && q.MaxLen > 0 && l > q.MaxLen {
			rep.add("tlv", "error", fmt.Sprintf("TLV %s longer than %d", q.Tag, q.MaxLen))
		}
	}
	for _, rule := range pack.TONNPIRules {
		if rule.SourcePattern != "" && matches(rule.SourcePattern, req.From) {
			a := protocol.AutoDetectAddress(protocol.Address{Addr: req.From})
			if rule.SourceTON != 0 && a.TON != rule.SourceTON {
				rep.add("ton_npi", "warning", fmt.Sprintf("source TON expected %v got %v", rule.SourceTON, a.TON))
			}
			if rule.SourceNPI != 0 && a.NPI != rule.SourceNPI {
				rep.add("ton_npi", "warning", fmt.Sprintf("source NPI expected %v got %v", rule.SourceNPI, a.NPI))
			}
		}
		if rule.DestPattern != "" && matches(rule.DestPattern, req.To) {
			a := protocol.AutoDetectAddress(protocol.Address{Addr: req.To})
			if rule.DestTON != 0 && a.TON != rule.DestTON {
				rep.add("ton_npi", "warning", fmt.Sprintf("dest TON expected %v got %v", rule.DestTON, a.TON))
			}
			if rule.DestNPI != 0 && a.NPI != rule.DestNPI {
				rep.add("ton_npi", "warning", fmt.Sprintf("dest NPI expected %v got %v", rule.DestNPI, a.NPI))
			}
		}
	}
	return rep
}
func (r *Report) add(c, s, m string) {
	if s == "error" {
		r.Passed = false
	}
	r.Findings = append(r.Findings, Finding{Case: c, Severity: s, Message: m})
}
func matches(pattern, s string) bool {
	if strings.HasPrefix(pattern, "regex:") {
		ok, _ := regexp.MatchString(strings.TrimPrefix(pattern, "regex:"), s)
		return ok
	}
	return strings.HasPrefix(s, pattern)
}
