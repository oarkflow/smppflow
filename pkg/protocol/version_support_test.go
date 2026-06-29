package protocol

import "testing"

func TestOperationMatrixCoversAllKnownCommands(t *testing.T) {
	must := []CommandID{GenericNACK, BindReceiver, BindTransmitter, BindTransceiver, Outbind, QuerySM, SubmitSM, DeliverSM, Unbind, ReplaceSM, CancelSM, EnquireLink, SubmitMulti, AlertNotification, DataSM, BroadcastSM, QueryBroadcastSM, CancelBroadcastSM}
	for _, cmd := range must {
		if _, ok := OperationByCommand(cmd); !ok {
			t.Fatalf("operation matrix missing %s", cmd)
		}
	}
	if SupportsCommand(Version33, BindTransceiver) {
		t.Fatal("3.3 must not advertise transceiver")
	}
	if !SupportsCommand(Version34, DataSM) {
		t.Fatal("3.4 must support data_sm")
	}
	if SupportsCommand(Version34, BroadcastSM) {
		t.Fatal("3.4 must not advertise broadcast_sm")
	}
	if !SupportsCommand(Version50, BroadcastSM) {
		t.Fatal("5.0 must support broadcast_sm")
	}
}

func TestRareOperationBuildersParse(t *testing.T) {
	src := Address{TON: TONAlphanumeric, NPI: NPIUnknown, Addr: "SRC"}
	dst := Address{TON: TONInternational, NPI: NPIISDN, Addr: "9779800000000"}
	outbind, err := ParseOutbind(BuildOutbind(1, OutbindRequest{SystemID: "smsc", Password: "pw"}))
	if err != nil || outbind.SystemID != "smsc" {
		t.Fatalf("outbind parse failed: %+v %v", outbind, err)
	}
	alert, err := ParseAlertNotification(BuildAlertNotification(2, AlertNotificationRequest{Source: dst, Esme: src, TLVs: []TLV{TLVByte(TLVMSAvailabilityStatus, 1)}}))
	if err != nil || alert.Source.Addr != dst.Addr {
		t.Fatalf("alert parse failed: %+v %v", alert, err)
	}
	bcast := BuildBroadcastSM(3, BroadcastSMRequest{ServiceType: "cb", Source: src, MessageID: "b1", DataCoding: DataCodingDefault, ShortMessage: []byte("broadcast"), TLVs: []TLV{TLVByte(TLVMessageState, 2)}})
	bp, err := ParseBroadcastSM(bcast)
	if err != nil || bp.MessageID != "b1" || string(bp.ShortMessage) != "broadcast" {
		t.Fatalf("broadcast parse failed: %+v %v", bp, err)
	}
	q, err := ParseQueryBroadcastSM(BuildQueryBroadcastSM(4, QueryBroadcastSMRequest{MessageID: "b1", Source: src}))
	if err != nil || q.MessageID != "b1" {
		t.Fatalf("query broadcast parse failed: %+v %v", q, err)
	}
	c, err := ParseCancelBroadcastSM(BuildCancelBroadcastSM(5, CancelBroadcastSMRequest{ServiceType: "cb", MessageID: "b1", Source: src}))
	if err != nil || c.MessageID != "b1" {
		t.Fatalf("cancel broadcast parse failed: %+v %v", c, err)
	}
}
