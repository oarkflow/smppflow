package protocol

// Operation describes one SMPP command pair and the protocol versions that define it.
type Operation struct {
	Command  CommandID
	Response CommandID
	Name     string
	Versions []Version
	Notes    string
}

// AllOperations is the executable coverage matrix for SMPP 3.3, 3.4 and 5.0.
// It intentionally includes rarely implemented operations such as outbind,
// alert_notification and broadcast commands so tests/examples can assert full coverage.
var AllOperations = []Operation{
	{BindReceiver, BindReceiverResp, "bind_receiver", []Version{Version33, Version34, Version50}, "ESME binds as receiver"},
	{BindTransmitter, BindTransmitterResp, "bind_transmitter", []Version{Version33, Version34, Version50}, "ESME binds as transmitter"},
	{BindTransceiver, BindTransceiverResp, "bind_transceiver", []Version{Version34, Version50}, "ESME binds as transceiver"},
	{Outbind, 0, "outbind", []Version{Version34, Version50}, "SMSC initiated bind request"},
	{Unbind, UnbindResp, "unbind", []Version{Version33, Version34, Version50}, "Graceful session termination"},
	{EnquireLink, EnquireLinkResp, "enquire_link", []Version{Version33, Version34, Version50}, "Heartbeat"},
	{GenericNACK, 0, "generic_nack", []Version{Version33, Version34, Version50}, "Generic negative acknowledgement"},
	{SubmitSM, SubmitSMResp, "submit_sm", []Version{Version33, Version34, Version50}, "Single destination submit"},
	{SubmitMulti, SubmitMultiResp, "submit_multi", []Version{Version34, Version50}, "Multiple destination submit"},
	{DeliverSM, DeliverSMResp, "deliver_sm", []Version{Version33, Version34, Version50}, "MO message or DLR delivery"},
	{DataSM, DataSMResp, "data_sm", []Version{Version34, Version50}, "Lightweight data submit/delivery with payload in TLVs"},
	{QuerySM, QuerySMResp, "query_sm", []Version{Version33, Version34, Version50}, "Query message status"},
	{ReplaceSM, ReplaceSMResp, "replace_sm", []Version{Version33, Version34, Version50}, "Replace scheduled message"},
	{CancelSM, CancelSMResp, "cancel_sm", []Version{Version33, Version34, Version50}, "Cancel scheduled or pending message"},
	{AlertNotification, 0, "alert_notification", []Version{Version34, Version50}, "SMSC alerts ESME that MS is available"},
	{BroadcastSM, BroadcastSMResp, "broadcast_sm", []Version{Version50}, "Cell broadcast submit"},
	{QueryBroadcastSM, QueryBroadcastSMResp, "query_broadcast_sm", []Version{Version50}, "Query cell broadcast"},
	{CancelBroadcastSM, CancelBroadcastSMResp, "cancel_broadcast_sm", []Version{Version50}, "Cancel cell broadcast"},
}

func SupportsCommand(v Version, cmd CommandID) bool {
	for _, op := range AllOperations {
		if op.Command == cmd || op.Response == cmd {
			for _, vv := range op.Versions {
				if vv == v {
					return true
				}
			}
			return false
		}
	}
	return false
}

func OperationByCommand(cmd CommandID) (Operation, bool) {
	for _, op := range AllOperations {
		if op.Command == cmd || op.Response == cmd {
			return op, true
		}
	}
	return Operation{}, false
}

func CommandsForVersion(v Version) []Operation {
	var out []Operation
	for _, op := range AllOperations {
		for _, vv := range op.Versions {
			if vv == v {
				out = append(out, op)
				break
			}
		}
	}
	return out
}
