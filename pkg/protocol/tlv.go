package protocol

import "fmt"

type TLVTag uint16

const (
	TLVDestAddrSubunit          TLVTag = 0x0005
	TLVDestNetworkType          TLVTag = 0x0006
	TLVDestBearerType           TLVTag = 0x0007
	TLVDestTelematicsID         TLVTag = 0x0008
	TLVSourceAddrSubunit        TLVTag = 0x000D
	TLVSourceNetworkType        TLVTag = 0x000E
	TLVSourceBearerType         TLVTag = 0x000F
	TLVSourceTelematicsID       TLVTag = 0x0010
	TLVQosTimeToLive            TLVTag = 0x0017
	TLVPayloadType              TLVTag = 0x0019
	TLVAdditionalStatusInfoText TLVTag = 0x001D
	TLVReceiptedMessageID       TLVTag = 0x001E
	TLVMSMsgWaitFacilities      TLVTag = 0x0030
	TLVPrivacyIndicator         TLVTag = 0x0201
	TLVSourceSubaddress         TLVTag = 0x0202
	TLVDestSubaddress           TLVTag = 0x0203
	TLVUserMessageReference     TLVTag = 0x0204
	TLVUserResponseCode         TLVTag = 0x0205
	TLVSourcePort               TLVTag = 0x020A
	TLVDestinationPort          TLVTag = 0x020B
	TLVSARMsgRefNum             TLVTag = 0x020C
	TLVLanguageIndicator        TLVTag = 0x020D
	TLVSARTotalSegments         TLVTag = 0x020E
	TLVSARSegmentSeqnum         TLVTag = 0x020F
	TLVSCInterfaceVersion       TLVTag = 0x0210
	TLVCallbackNumPresInd       TLVTag = 0x0302
	TLVCallbackNumAtag          TLVTag = 0x0303
	TLVNumberOfMessages         TLVTag = 0x0304
	TLVCallbackNum              TLVTag = 0x0381
	TLVDPFResult                TLVTag = 0x0420
	TLVSetDPF                   TLVTag = 0x0421
	TLVMSAvailabilityStatus     TLVTag = 0x0422
	TLVNetworkErrorCode         TLVTag = 0x0423
	TLVMessagePayload           TLVTag = 0x0424
	TLVDeliveryFailureReason    TLVTag = 0x0425
	TLVMoreMessagesToSend       TLVTag = 0x0426
	TLVMessageState             TLVTag = 0x0427
	TLVUSSDServiceOp            TLVTag = 0x0501
	TLVDisplayTime              TLVTag = 0x1201
	TLVSmsSignal                TLVTag = 0x1203
	TLVMSValidity               TLVTag = 0x1204
	TLVAlertOnMessageDelivery   TLVTag = 0x130C
	TLVItsReplyType             TLVTag = 0x1380
	TLVItsSessionInfo           TLVTag = 0x1383
)

type TLV struct {
	Tag   TLVTag
	Value []byte
}

func (t TLVTag) String() string {
	switch t {
	case TLVReceiptedMessageID:
		return "receipted_message_id"
	case TLVMessageState:
		return "message_state"
	case TLVNetworkErrorCode:
		return "network_error_code"
	case TLVMessagePayload:
		return "message_payload"
	case TLVSARMsgRefNum:
		return "sar_msg_ref_num"
	case TLVSARTotalSegments:
		return "sar_total_segments"
	case TLVSARSegmentSeqnum:
		return "sar_segment_seqnum"
	case TLVSCInterfaceVersion:
		return "sc_interface_version"
	default:
		return fmt.Sprintf("tlv_0x%04x", uint16(t))
	}
}

func TLVByte(tag TLVTag, v byte) TLV      { return TLV{Tag: tag, Value: []byte{v}} }
func TLVUint16(tag TLVTag, v uint16) TLV  { return TLV{Tag: tag, Value: []byte{byte(v >> 8), byte(v)}} }
func TLVCString(tag TLVTag, s string) TLV { b := append([]byte(s), 0); return TLV{Tag: tag, Value: b} }
func TLVBytes(tag TLVTag, b []byte) TLV {
	cp := append([]byte(nil), b...)
	return TLV{Tag: tag, Value: cp}
}

const (
	TLVCongestionState       TLVTag = 0x0301
	TLVBillingIdentification TLVTag = 0x060B
	TLVSourceNetworkID       TLVTag = 0x060D
	TLVSourceNodeID          TLVTag = 0x060F
	TLVDestNetworkID         TLVTag = 0x0611
	TLVDestNodeID            TLVTag = 0x0613
	TLVDestAddrNPCountry     TLVTag = 0x0615
	TLVDestAddrNPInformation TLVTag = 0x0616
	TLVDestAddrNPResolution  TLVTag = 0x0617
)

func TLVUint32(tag TLVTag, v uint32) TLV {
	return TLV{Tag: tag, Value: []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}}
}
func TLVInt(tag TLVTag, v int) TLV { return TLVUint32(tag, uint32(v)) }
func TLVBool(tag TLVTag, v bool) TLV {
	if v {
		return TLVByte(tag, 1)
	}
	return TLVByte(tag, 0)
}
