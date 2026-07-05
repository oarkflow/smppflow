package provider

import (
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/smppflow/pkg/protocol"
)

type Capability string

const (
	CapDLR     Capability = "dlr"
	CapMO      Capability = "mo"
	CapQuerySM Capability = "query_sm"
	CapTLS     Capability = "tls"
	CapSAR     Capability = "sar"
	CapUDH     Capability = "udh"
)

type ErrorRule struct {
	Code        string `json:"code"`
	Class       string `json:"class"`
	Retryable   bool   `json:"retryable"`
	Description string `json:"description"`
}
type CertificationProfile struct {
	Profile      Profile           `json:"profile"`
	Capabilities []Capability      `json:"capabilities"`
	RequiredTLVs []protocol.TLV    `json:"required_tlvs"`
	TONNPI       map[string]string `json:"ton_npi"`
	DLRExamples  []string          `json:"dlr_examples"`
	ErrorRules   []ErrorRule       `json:"error_rules"`
	CertifiedAt  time.Time         `json:"certified_at"`
	Version      string            `json:"version"`
}

func NewCertificationProfile(p Profile) CertificationProfile {
	caps := []Capability{CapDLR, CapMO, CapUDH}
	if p.SupportsQuery {
		caps = append(caps, CapQuerySM)
	}
	if p.UseTLS {
		caps = append(caps, CapTLS)
	}
	if p.Multipart == MultipartSAR {
		caps = append(caps, CapSAR)
	}
	return CertificationProfile{Profile: p, Capabilities: caps, TONNPI: map[string]string{"source": tonnpi(p.SourceTON, p.SourceNPI), "destination": tonnpi(p.DestTON, p.DestNPI)}, DLRExamples: []string{"id:abc123 sub:001 dlvrd:001 submit date:2401011200 done date:2401011201 stat:DELIVRD err:000 text:ok"}, ErrorRules: []ErrorRule{{Code: "ESME_RTHROTTLED", Class: "throttled", Retryable: true, Description: "slow down and retry"}, {Code: "ESME_RINVDSTADR", Class: "invalid_address", Retryable: false, Description: "invalid destination"}}, CertifiedAt: time.Now(), Version: "1.0"}
}
func tonnpi(ton protocol.TON, npi protocol.NPI) string { return fmt.Sprintf("%d:%d", ton, npi) }
func WithVendorQuirks(p Profile, vendor string) Profile {
	if p.Meta == nil {
		p.Meta = map[string]string{}
	}
	p.Meta["vendor"] = vendor
	switch strings.ToLower(vendor) {
	case "syniverse":
		p.DLR = DLRMixed
		p.MessageIDFormat = "hex"
	case "twilio":
		p.DLR = DLRTLV
		p.SupportsQuery = false
	case "infobip":
		p.Multipart = MultipartSAR
		p.SupportsDataSM = true
	case "generic-tls":
		p.UseTLS = true
	}
	return p
}
