package protocol

import (
	"fmt"
	"strings"
)

type BindMode string

const (
	BindModeReceiver    BindMode = "receiver"
	BindModeTransmitter BindMode = "transmitter"
	BindModeTransceiver BindMode = "transceiver"
)

type BindRequest struct {
	Mode             BindMode
	SystemID         string
	Password         string
	SystemType       string
	InterfaceVersion Version
	AddrTON          TON
	AddrNPI          NPI
	AddressRange     string
}

type BindResp struct {
	SystemID         string
	InterfaceVersion Version
}

func BindCommand(mode BindMode) CommandID {
	switch mode {
	case BindModeReceiver:
		return BindReceiver
	case BindModeTransmitter:
		return BindTransmitter
	default:
		return BindTransceiver
	}
}
func BindRespCommand(cmd CommandID) CommandID {
	switch cmd {
	case BindReceiver:
		return BindReceiverResp
	case BindTransmitter:
		return BindTransmitterResp
	default:
		return BindTransceiverResp
	}
}
func BuildBind(seq uint32, req BindRequest) PDU {
	if req.InterfaceVersion == 0 {
		req.InterfaceVersion = Version34
	}
	w := newWriter(64)
	w.CString(req.SystemID)
	w.CString(req.Password)
	w.CString(req.SystemType)
	w.U8(byte(req.InterfaceVersion))
	w.U8(byte(req.AddrTON))
	w.U8(byte(req.AddrNPI))
	w.CString(req.AddressRange)
	return PDU{CommandID: BindCommand(req.Mode), SequenceNumber: seq, Body: w.B}
}
func ParseBind(p PDU) (BindRequest, error) {
	r := newReader(p.Body)
	sid, err := r.CString()
	if err != nil {
		return BindRequest{}, err
	}
	pw, err := r.CString()
	if err != nil {
		return BindRequest{}, err
	}
	st, err := r.CString()
	if err != nil {
		return BindRequest{}, err
	}
	ver, err := r.U8()
	if err != nil {
		return BindRequest{}, err
	}
	ton, err := r.U8()
	if err != nil {
		return BindRequest{}, err
	}
	npi, err := r.U8()
	if err != nil {
		return BindRequest{}, err
	}
	rangeAddr, err := r.CString()
	if err != nil {
		return BindRequest{}, err
	}
	mode := BindModeTransceiver
	if p.CommandID == BindReceiver {
		mode = BindModeReceiver
	}
	if p.CommandID == BindTransmitter {
		mode = BindModeTransmitter
	}
	return BindRequest{Mode: mode, SystemID: sid, Password: pw, SystemType: st, InterfaceVersion: Version(ver), AddrTON: TON(ton), AddrNPI: NPI(npi), AddressRange: rangeAddr}, nil
}
func BuildBindResp(seq uint32, cmd CommandID, status CommandStatus, systemID string, version Version) PDU {
	w := newWriter(32)
	w.CString(systemID)
	tlvs := []TLV(nil)
	if version != 0 {
		tlvs = append(tlvs, TLVByte(TLVSCInterfaceVersion, byte(version)))
	}
	return PDU{CommandID: BindRespCommand(cmd), CommandStatus: status, SequenceNumber: seq, Body: w.B, TLVs: tlvs}
}
func ParseBindResp(p PDU) (BindResp, error) {
	r := newReader(p.Body)
	sid, err := r.CString()
	if err != nil {
		return BindResp{}, err
	}
	return BindResp{SystemID: sid}, nil
}

type Address struct {
	TON  TON
	NPI  NPI
	Addr string
}
type SubmitSMRequest struct {
	ServiceType          string
	Source               Address
	Destination          Address
	ESMClass             byte
	ProtocolID           byte
	PriorityFlag         byte
	ScheduleDeliveryTime string
	ValidityPeriod       string
	RegisteredDelivery   byte
	ReplaceIfPresent     byte
	DataCoding           byte
	SMDefaultMsgID       byte
	ShortMessage         []byte
	TLVs                 []TLV
}
type SubmitSMRespData struct{ MessageID string }

func BuildSubmitSM(seq uint32, req SubmitSMRequest) PDU {
	req.Source = AutoDetectAddress(req.Source)
	req.Destination = AutoDetectAddress(req.Destination)
	w := newWriter(128 + len(req.ShortMessage))
	w.CString(req.ServiceType)
	w.U8(byte(req.Source.TON))
	w.U8(byte(req.Source.NPI))
	w.CString(req.Source.Addr)
	w.U8(byte(req.Destination.TON))
	w.U8(byte(req.Destination.NPI))
	w.CString(req.Destination.Addr)
	w.U8(req.ESMClass)
	w.U8(req.ProtocolID)
	w.U8(req.PriorityFlag)
	w.CString(req.ScheduleDeliveryTime)
	w.CString(req.ValidityPeriod)
	w.U8(req.RegisteredDelivery)
	w.U8(req.ReplaceIfPresent)
	w.U8(req.DataCoding)
	w.U8(req.SMDefaultMsgID)
	w.LenU8Bytes(req.ShortMessage)
	return PDU{CommandID: SubmitSM, SequenceNumber: seq, Body: w.B, TLVs: req.TLVs}
}
func ParseSubmitSM(p PDU) (SubmitSMRequest, error) {
	r := newReader(p.Body)
	var req SubmitSMRequest
	var err error
	var b byte
	if req.ServiceType, err = r.CString(); err != nil {
		return req, err
	}
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Source.TON = TON(b)
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Source.NPI = NPI(b)
	if req.Source.Addr, err = r.CString(); err != nil {
		return req, err
	}
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Destination.TON = TON(b)
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Destination.NPI = NPI(b)
	if req.Destination.Addr, err = r.CString(); err != nil {
		return req, err
	}
	if req.ESMClass, err = r.U8(); err != nil {
		return req, err
	}
	if req.ProtocolID, err = r.U8(); err != nil {
		return req, err
	}
	if req.PriorityFlag, err = r.U8(); err != nil {
		return req, err
	}
	if req.ScheduleDeliveryTime, err = r.CString(); err != nil {
		return req, err
	}
	if req.ValidityPeriod, err = r.CString(); err != nil {
		return req, err
	}
	if req.RegisteredDelivery, err = r.U8(); err != nil {
		return req, err
	}
	if req.ReplaceIfPresent, err = r.U8(); err != nil {
		return req, err
	}
	if req.DataCoding, err = r.U8(); err != nil {
		return req, err
	}
	if req.SMDefaultMsgID, err = r.U8(); err != nil {
		return req, err
	}
	if req.ShortMessage, err = r.LenU8Bytes(); err != nil {
		return req, err
	}
	req.ShortMessage = append([]byte(nil), req.ShortMessage...)
	for r.Rem() > 0 {
		if r.Rem() < 4 {
			return req, fmt.Errorf("truncated tlv")
		}
		tag, _ := r.U16()
		l, _ := r.U16()
		v, e := r.Bytes(int(l))
		if e != nil {
			return req, e
		}
		req.TLVs = append(req.TLVs, TLV{Tag: TLVTag(tag), Value: append([]byte(nil), v...)})
	}
	return req, nil
}
func BuildSubmitSMResp(seq uint32, status CommandStatus, id string) PDU {
	w := newWriter(len(id) + 1)
	w.CString(id)
	return PDU{CommandID: SubmitSMResp, CommandStatus: status, SequenceNumber: seq, Body: w.B}
}
func ParseSubmitSMResp(p PDU) (SubmitSMRespData, error) {
	r := newReader(p.Body)
	id, err := r.CString()
	return SubmitSMRespData{MessageID: id}, err
}

func BuildDeliverSM(seq uint32, req SubmitSMRequest) PDU {
	p := BuildSubmitSM(seq, req)
	p.CommandID = DeliverSM
	return p
}
func BuildDeliverSMResp(seq uint32, status CommandStatus) PDU {
	return PDU{CommandID: DeliverSMResp, CommandStatus: status, SequenceNumber: seq}
}
func BuildEnquireLink(seq uint32) PDU {
	return PDU{CommandID: EnquireLink, SequenceNumber: seq}
}
func BuildEnquireLinkResp(seq uint32) PDU {
	return PDU{CommandID: EnquireLinkResp, SequenceNumber: seq}
}
func BuildUnbind(seq uint32) PDU { return PDU{CommandID: Unbind, SequenceNumber: seq} }
func BuildUnbindResp(seq uint32) PDU {
	return PDU{CommandID: UnbindResp, SequenceNumber: seq}
}
func BuildGenericNACK(seq uint32, status CommandStatus) PDU {
	return PDU{CommandID: GenericNACK, CommandStatus: status, SequenceNumber: seq}
}

func TLVStringValue(tlvs []TLV, tag TLVTag) string {
	for _, t := range tlvs {
		if t.Tag == tag {
			return strings.TrimRight(string(t.Value), "\x00")
		}
	}
	return ""
}
func TLVByteValue(tlvs []TLV, tag TLVTag) (byte, bool) {
	for _, t := range tlvs {
		if t.Tag == tag && len(t.Value) > 0 {
			return t.Value[0], true
		}
	}
	return 0, false
}

type QuerySMRequest struct {
	MessageID string
	Source    Address
}
type QuerySMRespData struct {
	MessageID    string
	FinalDate    string
	MessageState byte
	ErrorCode    byte
}

func BuildQuerySM(seq uint32, req QuerySMRequest) PDU {
	req.Source = AutoDetectAddress(req.Source)
	w := newWriter(64)
	w.CString(req.MessageID)
	w.U8(byte(req.Source.TON))
	w.U8(byte(req.Source.NPI))
	w.CString(req.Source.Addr)
	return PDU{CommandID: QuerySM, SequenceNumber: seq, Body: w.B}
}
func ParseQuerySM(p PDU) (QuerySMRequest, error) {
	r := newReader(p.Body)
	id, err := r.CString()
	if err != nil {
		return QuerySMRequest{}, err
	}
	ton, err := r.U8()
	if err != nil {
		return QuerySMRequest{}, err
	}
	npi, err := r.U8()
	if err != nil {
		return QuerySMRequest{}, err
	}
	addr, err := r.CString()
	if err != nil {
		return QuerySMRequest{}, err
	}
	return QuerySMRequest{MessageID: id, Source: Address{TON: TON(ton), NPI: NPI(npi), Addr: addr}}, nil
}
func BuildQuerySMResp(seq uint32, status CommandStatus, data QuerySMRespData) PDU {
	w := newWriter(64)
	w.CString(data.MessageID)
	w.CString(data.FinalDate)
	w.U8(data.MessageState)
	w.U8(data.ErrorCode)
	return PDU{CommandID: QuerySMResp, CommandStatus: status, SequenceNumber: seq, Body: w.B}
}
func ParseQuerySMResp(p PDU) (QuerySMRespData, error) {
	r := newReader(p.Body)
	id, err := r.CString()
	if err != nil {
		return QuerySMRespData{}, err
	}
	fd, err := r.CString()
	if err != nil {
		return QuerySMRespData{}, err
	}
	st, err := r.U8()
	if err != nil {
		return QuerySMRespData{}, err
	}
	ec, err := r.U8()
	if err != nil {
		return QuerySMRespData{}, err
	}
	return QuerySMRespData{MessageID: id, FinalDate: fd, MessageState: st, ErrorCode: ec}, nil
}

type ReplaceSMRequest struct {
	MessageID            string
	Source               Address
	ScheduleDeliveryTime string
	ValidityPeriod       string
	RegisteredDelivery   byte
	SMDefaultMsgID       byte
	DataCoding           byte
	ShortMessage         []byte
	TLVs                 []TLV
}

func BuildReplaceSM(seq uint32, req ReplaceSMRequest) PDU {
	req.Source = AutoDetectAddress(req.Source)
	w := newWriter(128 + len(req.ShortMessage))
	w.CString(req.MessageID)
	w.U8(byte(req.Source.TON))
	w.U8(byte(req.Source.NPI))
	w.CString(req.Source.Addr)
	w.CString(req.ScheduleDeliveryTime)
	w.CString(req.ValidityPeriod)
	w.U8(req.RegisteredDelivery)
	w.U8(req.SMDefaultMsgID)
	w.LenU8Bytes(req.ShortMessage)
	return PDU{CommandID: ReplaceSM, SequenceNumber: seq, Body: w.B, TLVs: req.TLVs}
}
func ParseReplaceSM(p PDU) (ReplaceSMRequest, error) {
	r := newReader(p.Body)
	var req ReplaceSMRequest
	var err error
	var b byte
	if req.MessageID, err = r.CString(); err != nil {
		return req, err
	}
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Source.TON = TON(b)
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Source.NPI = NPI(b)
	if req.Source.Addr, err = r.CString(); err != nil {
		return req, err
	}
	if req.ScheduleDeliveryTime, err = r.CString(); err != nil {
		return req, err
	}
	if req.ValidityPeriod, err = r.CString(); err != nil {
		return req, err
	}
	if req.RegisteredDelivery, err = r.U8(); err != nil {
		return req, err
	}
	if req.SMDefaultMsgID, err = r.U8(); err != nil {
		return req, err
	}
	if req.ShortMessage, err = r.LenU8Bytes(); err != nil {
		return req, err
	}
	req.ShortMessage = append([]byte(nil), req.ShortMessage...)
	return req, nil
}
func BuildReplaceSMResp(seq uint32, status CommandStatus) PDU {
	return PDU{CommandID: ReplaceSMResp, CommandStatus: status, SequenceNumber: seq}
}

type CancelSMRequest struct {
	ServiceType string
	MessageID   string
	Source      Address
	Destination Address
}

func BuildCancelSM(seq uint32, req CancelSMRequest) PDU {
	req.Source = AutoDetectAddress(req.Source)
	req.Destination = AutoDetectAddress(req.Destination)
	w := newWriter(96)
	w.CString(req.ServiceType)
	w.CString(req.MessageID)
	w.U8(byte(req.Source.TON))
	w.U8(byte(req.Source.NPI))
	w.CString(req.Source.Addr)
	w.U8(byte(req.Destination.TON))
	w.U8(byte(req.Destination.NPI))
	w.CString(req.Destination.Addr)
	return PDU{CommandID: CancelSM, SequenceNumber: seq, Body: w.B}
}
func ParseCancelSM(p PDU) (CancelSMRequest, error) {
	r := newReader(p.Body)
	var req CancelSMRequest
	var err error
	var b byte
	if req.ServiceType, err = r.CString(); err != nil {
		return req, err
	}
	if req.MessageID, err = r.CString(); err != nil {
		return req, err
	}
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Source.TON = TON(b)
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Source.NPI = NPI(b)
	if req.Source.Addr, err = r.CString(); err != nil {
		return req, err
	}
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Destination.TON = TON(b)
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Destination.NPI = NPI(b)
	if req.Destination.Addr, err = r.CString(); err != nil {
		return req, err
	}
	return req, nil
}
func BuildCancelSMResp(seq uint32, status CommandStatus) PDU {
	return PDU{CommandID: CancelSMResp, CommandStatus: status, SequenceNumber: seq}
}

func BuildDataSM(seq uint32, req SubmitSMRequest) PDU {
	p := BuildSubmitSM(seq, req)
	p.CommandID = DataSM
	return p
}
func BuildDataSMResp(seq uint32, status CommandStatus, id string) PDU {
	w := newWriter(len(id) + 1)
	w.CString(id)
	return PDU{CommandID: DataSMResp, CommandStatus: status, SequenceNumber: seq, Body: w.B}
}
func ParseDataSMResp(p PDU) (SubmitSMRespData, error) { return ParseSubmitSMResp(PDU{Body: p.Body}) }

type SubmitMultiRequest struct {
	SubmitSMRequest
	Destinations []Address
}

func BuildSubmitMulti(seq uint32, req SubmitMultiRequest) PDU {
	req.Source = AutoDetectAddress(req.Source)
	req.Destination = AutoDetectAddress(req.Destination)
	if len(req.Destinations) == 0 {
		req.Destinations = []Address{req.Destination}
	}
	for i := range req.Destinations {
		req.Destinations[i] = AutoDetectAddress(req.Destinations[i])
	}
	w := newWriter(128 + len(req.ShortMessage) + len(req.Destinations)*24)
	w.CString(req.ServiceType)
	w.U8(byte(req.Source.TON))
	w.U8(byte(req.Source.NPI))
	w.CString(req.Source.Addr)
	w.U8(byte(len(req.Destinations)))
	for _, d := range req.Destinations {
		w.U8(1)
		w.U8(byte(d.TON))
		w.U8(byte(d.NPI))
		w.CString(d.Addr)
	}
	w.U8(req.ESMClass)
	w.U8(req.ProtocolID)
	w.U8(req.PriorityFlag)
	w.CString(req.ScheduleDeliveryTime)
	w.CString(req.ValidityPeriod)
	w.U8(req.RegisteredDelivery)
	w.U8(req.ReplaceIfPresent)
	w.U8(req.DataCoding)
	w.U8(req.SMDefaultMsgID)
	w.LenU8Bytes(req.ShortMessage)
	return PDU{CommandID: SubmitMulti, SequenceNumber: seq, Body: w.B, TLVs: req.TLVs}
}
func BuildSubmitMultiResp(seq uint32, status CommandStatus, id string) PDU {
	w := newWriter(len(id) + 2)
	w.CString(id)
	w.U8(0)
	return PDU{CommandID: SubmitMultiResp, CommandStatus: status, SequenceNumber: seq, Body: w.B}
}

func parseTrailingTLVs(r *reader) ([]TLV, error) {
	var tlvs []TLV
	for r.Rem() > 0 {
		if r.Rem() < 4 {
			return nil, fmt.Errorf("truncated tlv")
		}
		tag, _ := r.U16()
		l, _ := r.U16()
		v, err := r.Bytes(int(l))
		if err != nil {
			return nil, err
		}
		tlvs = append(tlvs, TLV{Tag: TLVTag(tag), Value: append([]byte(nil), v...)})
	}
	return tlvs, nil
}

func ParseSubmitMulti(p PDU) (SubmitMultiRequest, error) {
	r := newReader(p.Body)
	var req SubmitMultiRequest
	var err error
	var b byte
	if req.ServiceType, err = r.CString(); err != nil {
		return req, err
	}
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Source.TON = TON(b)
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Source.NPI = NPI(b)
	if req.Source.Addr, err = r.CString(); err != nil {
		return req, err
	}
	num, err := r.U8()
	if err != nil {
		return req, err
	}
	for i := 0; i < int(num); i++ {
		flag, err := r.U8()
		if err != nil {
			return req, err
		}
		if flag != 1 {
			return req, fmt.Errorf("distribution list destinations are not expanded by parser: flag=%d", flag)
		}
		if b, err = r.U8(); err != nil {
			return req, err
		}
		ton := TON(b)
		if b, err = r.U8(); err != nil {
			return req, err
		}
		npi := NPI(b)
		addr, err := r.CString()
		if err != nil {
			return req, err
		}
		req.Destinations = append(req.Destinations, Address{TON: ton, NPI: npi, Addr: addr})
	}
	if len(req.Destinations) > 0 {
		req.Destination = req.Destinations[0]
	}
	if req.ESMClass, err = r.U8(); err != nil {
		return req, err
	}
	if req.ProtocolID, err = r.U8(); err != nil {
		return req, err
	}
	if req.PriorityFlag, err = r.U8(); err != nil {
		return req, err
	}
	if req.ScheduleDeliveryTime, err = r.CString(); err != nil {
		return req, err
	}
	if req.ValidityPeriod, err = r.CString(); err != nil {
		return req, err
	}
	if req.RegisteredDelivery, err = r.U8(); err != nil {
		return req, err
	}
	if req.ReplaceIfPresent, err = r.U8(); err != nil {
		return req, err
	}
	if req.DataCoding, err = r.U8(); err != nil {
		return req, err
	}
	if req.SMDefaultMsgID, err = r.U8(); err != nil {
		return req, err
	}
	if req.ShortMessage, err = r.LenU8Bytes(); err != nil {
		return req, err
	}
	req.ShortMessage = append([]byte(nil), req.ShortMessage...)
	req.TLVs, err = parseTrailingTLVs(r)
	return req, err
}

type SubmitMultiUnsuccess struct {
	Destination     Address
	ErrorStatusCode CommandStatus
}
type SubmitMultiRespData struct {
	MessageID    string
	Unsuccessful []SubmitMultiUnsuccess
}

func ParseSubmitMultiResp(p PDU) (SubmitMultiRespData, error) {
	r := newReader(p.Body)
	id, err := r.CString()
	if err != nil {
		return SubmitMultiRespData{}, err
	}
	n, err := r.U8()
	if err != nil {
		return SubmitMultiRespData{}, err
	}
	out := SubmitMultiRespData{MessageID: id}
	for i := 0; i < int(n); i++ {
		flag, err := r.U8()
		if err != nil {
			return out, err
		}
		if flag != 1 {
			return out, fmt.Errorf("unsupported unsuccessful destination flag %d", flag)
		}
		ton, err := r.U8()
		if err != nil {
			return out, err
		}
		npi, err := r.U8()
		if err != nil {
			return out, err
		}
		addr, err := r.CString()
		if err != nil {
			return out, err
		}
		b1, err := r.U16()
		if err != nil {
			return out, err
		}
		b2, err := r.U16()
		if err != nil {
			return out, err
		}
		out.Unsuccessful = append(out.Unsuccessful, SubmitMultiUnsuccess{Destination: Address{TON: TON(ton), NPI: NPI(npi), Addr: addr}, ErrorStatusCode: CommandStatus(uint32(b1)<<16 | uint32(b2))})
	}
	return out, nil
}

type OutbindRequest struct {
	SystemID string
	Password string
}

func BuildOutbind(seq uint32, req OutbindRequest) PDU {
	w := newWriter(32)
	w.CString(req.SystemID)
	w.CString(req.Password)
	return PDU{CommandID: Outbind, SequenceNumber: seq, Body: w.B}
}
func ParseOutbind(p PDU) (OutbindRequest, error) {
	r := newReader(p.Body)
	sid, err := r.CString()
	if err != nil {
		return OutbindRequest{}, err
	}
	pw, err := r.CString()
	return OutbindRequest{SystemID: sid, Password: pw}, err
}

type AlertNotificationRequest struct {
	Source Address
	Esme   Address
	TLVs   []TLV
}

func BuildAlertNotification(seq uint32, req AlertNotificationRequest) PDU {
	req.Source = AutoDetectAddress(req.Source)
	req.Esme = AutoDetectAddress(req.Esme)
	w := newWriter(64)
	w.U8(byte(req.Source.TON))
	w.U8(byte(req.Source.NPI))
	w.CString(req.Source.Addr)
	w.U8(byte(req.Esme.TON))
	w.U8(byte(req.Esme.NPI))
	w.CString(req.Esme.Addr)
	return PDU{CommandID: AlertNotification, SequenceNumber: seq, Body: w.B, TLVs: req.TLVs}
}
func ParseAlertNotification(p PDU) (AlertNotificationRequest, error) {
	r := newReader(p.Body)
	var req AlertNotificationRequest
	b, err := r.U8()
	if err != nil {
		return req, err
	}
	req.Source.TON = TON(b)
	b, err = r.U8()
	if err != nil {
		return req, err
	}
	req.Source.NPI = NPI(b)
	req.Source.Addr, err = r.CString()
	if err != nil {
		return req, err
	}
	b, err = r.U8()
	if err != nil {
		return req, err
	}
	req.Esme.TON = TON(b)
	b, err = r.U8()
	if err != nil {
		return req, err
	}
	req.Esme.NPI = NPI(b)
	req.Esme.Addr, err = r.CString()
	if err != nil {
		return req, err
	}
	req.TLVs, err = parseTrailingTLVs(r)
	return req, err
}

type BroadcastSMRequest struct {
	ServiceType             string
	Source                  Address
	MessageID               string
	PriorityFlag            byte
	ScheduleDeliveryTime    string
	ValidityPeriod          string
	ReplaceIfPresent        byte
	DataCoding              byte
	SMDefaultMsgID          byte
	BroadcastAreaIdentifier []byte
	BroadcastContentType    []byte
	ShortMessage            []byte
	TLVs                    []TLV
}

func BuildBroadcastSM(seq uint32, req BroadcastSMRequest) PDU {
	req.Source = AutoDetectAddress(req.Source)
	w := newWriter(160 + len(req.ShortMessage))
	w.CString(req.ServiceType)
	w.U8(byte(req.Source.TON))
	w.U8(byte(req.Source.NPI))
	w.CString(req.Source.Addr)
	w.CString(req.MessageID)
	w.U8(req.PriorityFlag)
	w.CString(req.ScheduleDeliveryTime)
	w.CString(req.ValidityPeriod)
	w.U8(req.ReplaceIfPresent)
	w.U8(req.DataCoding)
	w.U8(req.SMDefaultMsgID)
	w.LenU8Bytes(req.ShortMessage)
	return PDU{CommandID: BroadcastSM, SequenceNumber: seq, Body: w.B, TLVs: req.TLVs}
}
func BuildBroadcastSMResp(seq uint32, status CommandStatus, id string) PDU {
	w := newWriter(len(id) + 1)
	w.CString(id)
	return PDU{CommandID: BroadcastSMResp, CommandStatus: status, SequenceNumber: seq, Body: w.B}
}
func ParseBroadcastSM(p PDU) (BroadcastSMRequest, error) {
	r := newReader(p.Body)
	var req BroadcastSMRequest
	var err error
	var b byte
	if req.ServiceType, err = r.CString(); err != nil {
		return req, err
	}
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Source.TON = TON(b)
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Source.NPI = NPI(b)
	if req.Source.Addr, err = r.CString(); err != nil {
		return req, err
	}
	if req.MessageID, err = r.CString(); err != nil {
		return req, err
	}
	if req.PriorityFlag, err = r.U8(); err != nil {
		return req, err
	}
	if req.ScheduleDeliveryTime, err = r.CString(); err != nil {
		return req, err
	}
	if req.ValidityPeriod, err = r.CString(); err != nil {
		return req, err
	}
	if req.ReplaceIfPresent, err = r.U8(); err != nil {
		return req, err
	}
	if req.DataCoding, err = r.U8(); err != nil {
		return req, err
	}
	if req.SMDefaultMsgID, err = r.U8(); err != nil {
		return req, err
	}
	if req.ShortMessage, err = r.LenU8Bytes(); err != nil {
		return req, err
	}
	req.ShortMessage = append([]byte(nil), req.ShortMessage...)
	req.TLVs, err = parseTrailingTLVs(r)
	return req, err
}
func ParseBroadcastSMResp(p PDU) (SubmitSMRespData, error) {
	return ParseSubmitSMResp(PDU{Body: p.Body})
}

type QueryBroadcastSMRequest struct {
	MessageID string
	Source    Address
}
type QueryBroadcastSMRespData struct {
	MessageID    string
	MessageState byte
}

func BuildQueryBroadcastSM(seq uint32, req QueryBroadcastSMRequest) PDU {
	req.Source = AutoDetectAddress(req.Source)
	w := newWriter(64)
	w.CString(req.MessageID)
	w.U8(byte(req.Source.TON))
	w.U8(byte(req.Source.NPI))
	w.CString(req.Source.Addr)
	return PDU{CommandID: QueryBroadcastSM, SequenceNumber: seq, Body: w.B}
}
func ParseQueryBroadcastSM(p PDU) (QueryBroadcastSMRequest, error) {
	q, err := ParseQuerySM(PDU{Body: p.Body})
	return QueryBroadcastSMRequest{MessageID: q.MessageID, Source: q.Source}, err
}
func BuildQueryBroadcastSMResp(seq uint32, status CommandStatus, data QueryBroadcastSMRespData) PDU {
	w := newWriter(32)
	w.CString(data.MessageID)
	w.U8(data.MessageState)
	return PDU{CommandID: QueryBroadcastSMResp, CommandStatus: status, SequenceNumber: seq, Body: w.B}
}
func ParseQueryBroadcastSMResp(p PDU) (QueryBroadcastSMRespData, error) {
	r := newReader(p.Body)
	id, err := r.CString()
	if err != nil {
		return QueryBroadcastSMRespData{}, err
	}
	st, err := r.U8()
	return QueryBroadcastSMRespData{MessageID: id, MessageState: st}, err
}

type CancelBroadcastSMRequest struct {
	ServiceType string
	MessageID   string
	Source      Address
}

func BuildCancelBroadcastSM(seq uint32, req CancelBroadcastSMRequest) PDU {
	req.Source = AutoDetectAddress(req.Source)
	w := newWriter(64)
	w.CString(req.ServiceType)
	w.CString(req.MessageID)
	w.U8(byte(req.Source.TON))
	w.U8(byte(req.Source.NPI))
	w.CString(req.Source.Addr)
	return PDU{CommandID: CancelBroadcastSM, SequenceNumber: seq, Body: w.B}
}
func ParseCancelBroadcastSM(p PDU) (CancelBroadcastSMRequest, error) {
	r := newReader(p.Body)
	var req CancelBroadcastSMRequest
	var err error
	var b byte
	if req.ServiceType, err = r.CString(); err != nil {
		return req, err
	}
	if req.MessageID, err = r.CString(); err != nil {
		return req, err
	}
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Source.TON = TON(b)
	if b, err = r.U8(); err != nil {
		return req, err
	}
	req.Source.NPI = NPI(b)
	if req.Source.Addr, err = r.CString(); err != nil {
		return req, err
	}
	return req, nil
}
func BuildCancelBroadcastSMResp(seq uint32, status CommandStatus) PDU {
	return PDU{CommandID: CancelBroadcastSMResp, CommandStatus: status, SequenceNumber: seq}
}
