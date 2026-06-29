package protocol

import "fmt"

type Version byte

const (
	Version33 Version = 0x33
	Version34 Version = 0x34
	Version50 Version = 0x50
)

func (v Version) String() string {
	switch v {
	case Version33:
		return "3.3"
	case Version34:
		return "3.4"
	case Version50:
		return "5.0"
	default:
		return fmt.Sprintf("0x%02x", byte(v))
	}
}

type CommandID uint32

const (
	GenericNACK           CommandID = 0x80000000
	BindReceiver          CommandID = 0x00000001
	BindReceiverResp      CommandID = 0x80000001
	BindTransmitter       CommandID = 0x00000002
	BindTransmitterResp   CommandID = 0x80000002
	QuerySM               CommandID = 0x00000003
	QuerySMResp           CommandID = 0x80000003
	SubmitSM              CommandID = 0x00000004
	SubmitSMResp          CommandID = 0x80000004
	DeliverSM             CommandID = 0x00000005
	DeliverSMResp         CommandID = 0x80000005
	Unbind                CommandID = 0x00000006
	UnbindResp            CommandID = 0x80000006
	ReplaceSM             CommandID = 0x00000007
	ReplaceSMResp         CommandID = 0x80000007
	CancelSM              CommandID = 0x00000008
	CancelSMResp          CommandID = 0x80000008
	BindTransceiver       CommandID = 0x00000009
	BindTransceiverResp   CommandID = 0x80000009
	Outbind               CommandID = 0x0000000B
	EnquireLink           CommandID = 0x00000015
	EnquireLinkResp       CommandID = 0x80000015
	SubmitMulti           CommandID = 0x00000021
	SubmitMultiResp       CommandID = 0x80000021
	AlertNotification     CommandID = 0x00000102
	DataSM                CommandID = 0x00000103
	DataSMResp            CommandID = 0x80000103
	BroadcastSM           CommandID = 0x00000111
	BroadcastSMResp       CommandID = 0x80000111
	QueryBroadcastSM      CommandID = 0x00000112
	QueryBroadcastSMResp  CommandID = 0x80000112
	CancelBroadcastSM     CommandID = 0x00000113
	CancelBroadcastSMResp CommandID = 0x80000113
)

func (c CommandID) IsResponse() bool { return uint32(c)&0x80000000 != 0 }

func (c CommandID) String() string {
	switch c {
	case GenericNACK:
		return "generic_nack"
	case BindReceiver:
		return "bind_receiver"
	case BindReceiverResp:
		return "bind_receiver_resp"
	case BindTransmitter:
		return "bind_transmitter"
	case BindTransmitterResp:
		return "bind_transmitter_resp"
	case BindTransceiver:
		return "bind_transceiver"
	case BindTransceiverResp:
		return "bind_transceiver_resp"
	case SubmitSM:
		return "submit_sm"
	case SubmitSMResp:
		return "submit_sm_resp"
	case DeliverSM:
		return "deliver_sm"
	case DeliverSMResp:
		return "deliver_sm_resp"
	case DataSM:
		return "data_sm"
	case DataSMResp:
		return "data_sm_resp"
	case QuerySM:
		return "query_sm"
	case QuerySMResp:
		return "query_sm_resp"
	case ReplaceSM:
		return "replace_sm"
	case ReplaceSMResp:
		return "replace_sm_resp"
	case CancelSM:
		return "cancel_sm"
	case CancelSMResp:
		return "cancel_sm_resp"
	case EnquireLink:
		return "enquire_link"
	case EnquireLinkResp:
		return "enquire_link_resp"
	case Unbind:
		return "unbind"
	case UnbindResp:
		return "unbind_resp"
	case SubmitMulti:
		return "submit_multi"
	case SubmitMultiResp:
		return "submit_multi_resp"
	case AlertNotification:
		return "alert_notification"
	case Outbind:
		return "outbind"
	case BroadcastSM:
		return "broadcast_sm"
	case BroadcastSMResp:
		return "broadcast_sm_resp"
	case QueryBroadcastSM:
		return "query_broadcast_sm"
	case QueryBroadcastSMResp:
		return "query_broadcast_sm_resp"
	case CancelBroadcastSM:
		return "cancel_broadcast_sm"
	case CancelBroadcastSMResp:
		return "cancel_broadcast_sm_resp"
	default:
		return fmt.Sprintf("unknown_0x%08x", uint32(c))
	}
}

type CommandStatus uint32

const (
	ESME_ROK              CommandStatus = 0x00000000
	ESME_RINVMSGLEN       CommandStatus = 0x00000001
	ESME_RINVCMDLEN       CommandStatus = 0x00000002
	ESME_RINVCMDID        CommandStatus = 0x00000003
	ESME_RINVBNDSTS       CommandStatus = 0x00000004
	ESME_RALYBND          CommandStatus = 0x00000005
	ESME_RINVPRTFLG       CommandStatus = 0x00000006
	ESME_RINVREGDLVFLG    CommandStatus = 0x00000007
	ESME_RSYSERR          CommandStatus = 0x00000008
	ESME_RINVSRCADR       CommandStatus = 0x0000000A
	ESME_RINVDSTADR       CommandStatus = 0x0000000B
	ESME_RINVMSGID        CommandStatus = 0x0000000C
	ESME_RBINDFAIL        CommandStatus = 0x0000000D
	ESME_RINVPASWD        CommandStatus = 0x0000000E
	ESME_RINVSYSID        CommandStatus = 0x0000000F
	ESME_RCANCELFAIL      CommandStatus = 0x00000011
	ESME_RREPLACEFAIL     CommandStatus = 0x00000013
	ESME_RMSGQFUL         CommandStatus = 0x00000014
	ESME_RINVSERTYP       CommandStatus = 0x00000015
	ESME_RINVNUMDESTS     CommandStatus = 0x00000033
	ESME_RINVDLNAME       CommandStatus = 0x00000034
	ESME_RINVDESTFLAG     CommandStatus = 0x00000040
	ESME_RINVSUBREP       CommandStatus = 0x00000042
	ESME_RINVESMCLASS     CommandStatus = 0x00000043
	ESME_RCNTSUBDL        CommandStatus = 0x00000044
	ESME_RSUBMITFAIL      CommandStatus = 0x00000045
	ESME_RINVSRCTON       CommandStatus = 0x00000048
	ESME_RINVSRCNPI       CommandStatus = 0x00000049
	ESME_RINVDSTTON       CommandStatus = 0x00000050
	ESME_RINVDSTNPI       CommandStatus = 0x00000051
	ESME_RINVSYSTYP       CommandStatus = 0x00000053
	ESME_RINVREPFLAG      CommandStatus = 0x00000054
	ESME_RINVNUMMSGS      CommandStatus = 0x00000055
	ESME_RTHROTTLED       CommandStatus = 0x00000058
	ESME_RINVSCHED        CommandStatus = 0x00000061
	ESME_RINVEXPIRY       CommandStatus = 0x00000062
	ESME_RINVDFTMSGID     CommandStatus = 0x00000063
	ESME_RX_T_APPN        CommandStatus = 0x00000064
	ESME_RX_P_APPN        CommandStatus = 0x00000065
	ESME_RX_R_APPN        CommandStatus = 0x00000066
	ESME_RQUERYFAIL       CommandStatus = 0x00000067
	ESME_RINVOPTPARSTREAM CommandStatus = 0x000000C0
	ESME_ROPTPARNOTALLWD  CommandStatus = 0x000000C1
	ESME_RINVPARLEN       CommandStatus = 0x000000C2
	ESME_RMISSINGOPTPARAM CommandStatus = 0x000000C3
	ESME_RINVOPTPARAMVAL  CommandStatus = 0x000000C4
	ESME_RDELIVERYFAILURE CommandStatus = 0x000000FE
	ESME_RUNKNOWNERR      CommandStatus = 0x000000FF
)

func (s CommandStatus) Temporary() bool {
	switch s {
	case ESME_ROK:
		return false
	case ESME_RSYSERR, ESME_RMSGQFUL, ESME_RTHROTTLED, ESME_RX_T_APPN, ESME_RX_P_APPN, ESME_RX_R_APPN:
		return true
	default:
		return false
	}
}

func (s CommandStatus) String() string {
	switch s {
	case ESME_ROK:
		return "OK"
	case ESME_RTHROTTLED:
		return "THROTTLED"
	case ESME_RSYSERR:
		return "SYSTEM_ERROR"
	case ESME_RINVSYSID:
		return "INVALID_SYSTEM_ID"
	case ESME_RINVPASWD:
		return "INVALID_PASSWORD"
	case ESME_RBINDFAIL:
		return "BIND_FAILED"
	case ESME_RSUBMITFAIL:
		return "SUBMIT_FAILED"
	case ESME_RINVDSTADR:
		return "INVALID_DESTINATION_ADDRESS"
	case ESME_RINVSRCADR:
		return "INVALID_SOURCE_ADDRESS"
	default:
		return fmt.Sprintf("0x%08x", uint32(s))
	}
}

type TON byte
type NPI byte

const (
	TONUnknown       TON = 0
	TONInternational TON = 1
	TONNational      TON = 2
	TONNetwork       TON = 3
	TONSubscriber    TON = 4
	TONAlphanumeric  TON = 5
	TONAbbreviated   TON = 6

	NPIUnknown  NPI = 0
	NPIISDN     NPI = 1
	NPIData     NPI = 3
	NPITelex    NPI = 4
	NPILandMob  NPI = 6
	NPINational NPI = 8
	NPIPrivate  NPI = 9
	NPIERMES    NPI = 10
	NPIInternet NPI = 14
	NPIWAP      NPI = 18
)

const (
	DataCodingDefault byte = 0x00
	DataCodingIA5     byte = 0x01
	DataCodingLatin1  byte = 0x03
	DataCodingBinary  byte = 0x04
	DataCodingUCS2    byte = 0x08
)

const (
	ESMClassUDHIndicator    byte = 0x40
	ESMClassDeliveryReceipt byte = 0x04
)
