package protocol

type PDU struct {
	CommandID      CommandID
	CommandStatus  CommandStatus
	SequenceNumber uint32
	Body           []byte
	TLVs           []TLV
}

func (p PDU) ResponseID() CommandID { return CommandID(uint32(p.CommandID) | 0x80000000) }
func (p PDU) IsResponse() bool      { return p.CommandID.IsResponse() }
