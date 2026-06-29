package provider

import (
	"time"

	"github.com/oarkflow/smppflow/protocol"
)

type MultipartMode string

const (
	MultipartUDH     MultipartMode = "udh"
	MultipartSAR     MultipartMode = "sar_tlv"
	MultipartPayload MultipartMode = "message_payload"
)

type DLRFormat string

const (
	DLRText  DLRFormat = "text"
	DLRTLV   DLRFormat = "tlv"
	DLRMixed DLRFormat = "mixed"
)

type Profile struct {
	ID                  string
	Name                string
	Version             protocol.Version
	Addr                string
	SystemID            string
	Password            string
	SystemType          string
	BindMode            protocol.BindMode
	SourceTON           protocol.TON
	SourceNPI           protocol.NPI
	DestTON             protocol.TON
	DestNPI             protocol.NPI
	TPS                 int
	WindowSize          int
	EnquireInterval     time.Duration
	ResponseTimeout     time.Duration
	Multipart           MultipartMode
	DLR                 DLRFormat
	MessageIDFormat     string
	SupportsQuery       bool
	SupportsCancel      bool
	SupportsReplace     bool
	SupportsDataSM      bool
	SupportsSubmitMulti bool
	UseTLS              bool
	TLVs                []protocol.TLV
	ErrorMap            map[protocol.CommandStatus]string
	Meta                map[string]string
}

func Generic34(id, addr, systemID, password string) Profile {
	return Profile{ID: id, Name: id, Version: protocol.Version34, Addr: addr, SystemID: systemID, Password: password, BindMode: protocol.BindModeTransceiver, SourceTON: protocol.TONAlphanumeric, SourceNPI: protocol.NPIUnknown, DestTON: protocol.TONInternational, DestNPI: protocol.NPIISDN, TPS: 100, WindowSize: 16, EnquireInterval: 30 * time.Second, ResponseTimeout: 10 * time.Second, Multipart: MultipartUDH, DLR: DLRMixed, SupportsQuery: true, SupportsCancel: true, SupportsReplace: true, SupportsDataSM: true, SupportsSubmitMulti: true}
}

func Legacy33(id, addr, systemID, password string) Profile {
	p := Generic34(id, addr, systemID, password)
	p.Version = protocol.Version33
	p.SupportsDataSM = false
	p.SupportsSubmitMulti = false
	p.Multipart = MultipartUDH
	return p
}

func Modern50(id, addr, systemID, password string) Profile {
	p := Generic34(id, addr, systemID, password)
	p.Version = protocol.Version50
	p.Multipart = MultipartSAR
	return p
}

func (p Profile) BindRequest() protocol.BindRequest {
	mode := p.BindMode
	if mode == "" {
		mode = protocol.BindModeTransceiver
	}
	version := p.Version
	if version == 0 {
		version = protocol.Version34
	}
	return protocol.BindRequest{Mode: mode, SystemID: p.SystemID, Password: p.Password, SystemType: p.SystemType, InterfaceVersion: version}
}
