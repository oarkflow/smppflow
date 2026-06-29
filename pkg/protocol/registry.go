package protocol

import (
	"fmt"
	"sort"
)

// Capability is the runtime feature matrix for a negotiated SMPP version.
type Capability struct {
	Version                   Version
	Commands                  map[CommandID]bool
	TLVs                      map[TLVTag]bool
	SupportsTransceiver       bool
	SupportsOptionalTLV       bool
	SupportsDataSM            bool
	SupportsSubmitMulti       bool
	SupportsCellBroadcast     bool
	SupportsUSSD              bool
	SupportsCongestionState   bool
	SupportsNumberPortability bool
}

func Capabilities(v Version) Capability {
	if v == 0 {
		v = Version34
	}
	c := Capability{Version: v, Commands: map[CommandID]bool{}, TLVs: map[TLVTag]bool{}}
	for _, op := range CommandsForVersion(v) {
		c.Commands[op.Command] = true
		if op.Response != 0 {
			c.Commands[op.Response] = true
		}
	}
	switch v {
	case Version33:
		c.SupportsOptionalTLV = false
	case Version34:
		c.SupportsTransceiver = true
		c.SupportsOptionalTLV = true
		c.SupportsDataSM = true
		c.SupportsSubmitMulti = true
	case Version50:
		c.SupportsTransceiver = true
		c.SupportsOptionalTLV = true
		c.SupportsDataSM = true
		c.SupportsSubmitMulti = true
		c.SupportsCellBroadcast = true
		c.SupportsUSSD = true
		c.SupportsCongestionState = true
		c.SupportsNumberPortability = true
	}
	for tag, meta := range TLVRegistry() {
		if meta.MinVersion == 0 || int(meta.MinVersion) <= int(v) {
			c.TLVs[tag] = true
		}
	}
	return c
}

func NegotiateVersion(client Version, supported ...Version) Version {
	if client == 0 {
		client = Version34
	}
	if len(supported) == 0 {
		supported = []Version{Version50, Version34, Version33}
	}
	best := Version33
	for _, v := range supported {
		if v == client {
			return v
		}
		if int(v) <= int(client) && int(v) > int(best) {
			best = v
		}
	}
	return best
}

func ValidatePDUForVersion(v Version, p PDU) error {
	cap := Capabilities(v)
	if !cap.Commands[p.CommandID] {
		return fmt.Errorf("%s is not supported by SMPP %s", p.CommandID, v)
	}
	if len(p.TLVs) > 0 && !cap.SupportsOptionalTLV {
		return fmt.Errorf("optional TLVs are not supported by SMPP %s", v)
	}
	for _, tlv := range p.TLVs {
		if !cap.TLVs[tlv.Tag] {
			return fmt.Errorf("TLV %s is not supported by SMPP %s", tlv.Tag, v)
		}
	}
	return nil
}

type TLVMeta struct {
	Tag        TLVTag
	Name       string
	MinVersion Version
	MaxLen     int
	Notes      string
}

func TLVRegistry() map[TLVTag]TLVMeta {
	reg := map[TLVTag]TLVMeta{}
	add := func(t TLVTag, name string, ver Version, max int, notes string) {
		reg[t] = TLVMeta{t, name, ver, max, notes}
	}
	add(TLVReceiptedMessageID, "receipted_message_id", Version34, 65, "DLR/provider id")
	add(TLVMessageState, "message_state", Version34, 1, "SMPP message state")
	add(TLVNetworkErrorCode, "network_error_code", Version34, 3, "network error")
	add(TLVMessagePayload, "message_payload", Version34, 0, "long payload")
	add(TLVSARMsgRefNum, "sar_msg_ref_num", Version34, 2, "multipart ref")
	add(TLVSARTotalSegments, "sar_total_segments", Version34, 1, "multipart total")
	add(TLVSARSegmentSeqnum, "sar_segment_seqnum", Version34, 1, "multipart seq")
	add(TLVUserMessageReference, "user_message_reference", Version34, 2, "client reference")
	add(TLVSourcePort, "source_port", Version34, 2, "application port")
	add(TLVDestinationPort, "destination_port", Version34, 2, "application port")
	add(TLVCallbackNum, "callback_num", Version34, 20, "callback")
	add(TLVCallbackNumPresInd, "callback_num_pres_ind", Version34, 1, "callback presentation")
	add(TLVCallbackNumAtag, "callback_num_atag", Version34, 65, "callback tag")
	add(TLVLanguageIndicator, "language_indicator", Version34, 1, "language")
	add(TLVItsSessionInfo, "its_session_info", Version34, 2, "interactive teleservice")
	add(TLVPrivacyIndicator, "privacy_indicator", Version34, 1, "privacy")
	add(TLVPayloadType, "payload_type", Version34, 1, "payload type")
	add(TLVMoreMessagesToSend, "more_messages_to_send", Version34, 1, "more messages")
	add(TLVMSMsgWaitFacilities, "ms_msg_wait_facilities", Version34, 1, "message waiting")
	add(TLVUSSDServiceOp, "ussd_service_op", Version50, 1, "USSD")
	add(TLVCongestionState, "congestion_state", Version50, 1, "peer load 0-100")
	add(TLVBillingIdentification, "billing_identification", Version50, 1024, "billing")
	add(TLVSourceNetworkID, "source_network_id", Version50, 65, "source endpoint")
	add(TLVSourceNodeID, "source_node_id", Version50, 65, "source endpoint")
	add(TLVDestNetworkID, "dest_network_id", Version50, 65, "destination endpoint")
	add(TLVDestNodeID, "dest_node_id", Version50, 65, "destination endpoint")
	add(TLVDestAddrNPCountry, "dest_addr_np_country", Version50, 3, "number portability")
	add(TLVDestAddrNPInformation, "dest_addr_np_information", Version50, 10, "number portability")
	add(TLVDestAddrNPResolution, "dest_addr_np_resolution", Version50, 1, "number portability")
	return reg
}

func RegisteredTLVs() []TLVMeta {
	reg := TLVRegistry()
	out := make([]TLVMeta, 0, len(reg))
	for _, m := range reg {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Tag < out[j].Tag })
	return out
}
