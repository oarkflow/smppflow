package message

import (
	"regexp"
	"strings"
	"time"

	"github.com/oarkflow/smppflow/lifecycle"
	"github.com/oarkflow/smppflow/protocol"
)

var dlrRE = regexp.MustCompile(`(?i)id:([^\s]+).*stat:([^\s]+).*err:([^\s]+)`)

func ParseDLR(raw string, tlvs []protocol.TLV) DeliveryReceipt {
	d := DeliveryReceipt{Raw: raw, DoneAt: time.Now(), State: lifecycle.MsgUnknown}
	if id := protocol.TLVStringValue(tlvs, protocol.TLVReceiptedMessageID); id != "" {
		d.ProviderMessageID = id
	}
	if st, ok := protocol.TLVByteValue(tlvs, protocol.TLVMessageState); ok {
		d.State = stateFromTLV(st)
	}
	m := dlrRE.FindStringSubmatch(raw)
	if len(m) == 4 {
		if d.ProviderMessageID == "" {
			d.ProviderMessageID = m[1]
		}
		if d.State == lifecycle.MsgUnknown {
			d.State = stateFromText(m[2])
		}
		d.ErrorCode = m[3]
	}
	return d
}
func stateFromTLV(b byte) lifecycle.MessageState {
	switch b {
	case 1:
		return lifecycle.MsgAccepted
	case 2:
		return lifecycle.MsgDelivered
	case 3:
		return lifecycle.MsgExpired
	case 4:
		return lifecycle.MsgUndeliverable
	case 5:
		return lifecycle.MsgAccepted
	case 6:
		return lifecycle.MsgUndeliverable
	case 7:
		return lifecycle.MsgUnknown
	case 8:
		return lifecycle.MsgRejected
	default:
		return lifecycle.MsgUnknown
	}
}
func stateFromText(s string) lifecycle.MessageState {
	switch strings.ToUpper(s) {
	case "DELIVRD", "DELIVERED":
		return lifecycle.MsgDelivered
	case "EXPIRED":
		return lifecycle.MsgExpired
	case "UNDELIV", "UNDELIVERABLE":
		return lifecycle.MsgUndeliverable
	case "REJECTD", "REJECTED":
		return lifecycle.MsgRejected
	case "ACCEPTD", "ACCEPTED", "ENROUTE":
		return lifecycle.MsgAccepted
	default:
		return lifecycle.MsgUnknown
	}
}
