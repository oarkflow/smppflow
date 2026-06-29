package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/oarkflow/smppflow/pkg/protocol"
)

const HeaderLen = 16
const MaxPDULength = 16 * 1024 * 1024

func Encode(p protocol.PDU) ([]byte, error) {
	bodyLen := len(p.Body)
	tlvLen := 0
	for _, t := range p.TLVs {
		tlvLen += 4 + len(t.Value)
	}
	if bodyLen+tlvLen+HeaderLen > MaxPDULength {
		return nil, fmt.Errorf("pdu too large: %d", bodyLen+tlvLen+HeaderLen)
	}
	w := NewWriter(HeaderLen + bodyLen + tlvLen)
	w.U32(uint32(HeaderLen + bodyLen + tlvLen))
	w.U32(uint32(p.CommandID))
	w.U32(uint32(p.CommandStatus))
	w.U32(p.SequenceNumber)
	w.Bytes(p.Body)
	for _, t := range p.TLVs {
		if len(t.Value) > 65535 {
			return nil, fmt.Errorf("tlv %s too large", t.Tag)
		}
		w.U16(uint16(t.Tag))
		w.U16(uint16(len(t.Value)))
		w.Bytes(t.Value)
	}
	return w.B, nil
}

func Decode(raw []byte) (protocol.PDU, error) {
	if len(raw) < HeaderLen {
		return protocol.PDU{}, errors.New("pdu shorter than header")
	}
	length := binary.BigEndian.Uint32(raw[0:4])
	if int(length) != len(raw) {
		return protocol.PDU{}, fmt.Errorf("pdu length mismatch header=%d actual=%d", length, len(raw))
	}
	if length < HeaderLen || length > MaxPDULength {
		return protocol.PDU{}, fmt.Errorf("invalid pdu length %d", length)
	}
	p := protocol.PDU{
		CommandID:      protocol.CommandID(binary.BigEndian.Uint32(raw[4:8])),
		CommandStatus:  protocol.CommandStatus(binary.BigEndian.Uint32(raw[8:12])),
		SequenceNumber: binary.BigEndian.Uint32(raw[12:16]),
	}
	p.Body = append([]byte(nil), raw[HeaderLen:]...)
	return p, nil
}

func ReadPDU(r io.Reader) (protocol.PDU, error) {
	header := make([]byte, HeaderLen)
	if _, err := io.ReadFull(r, header); err != nil {
		return protocol.PDU{}, err
	}
	length := binary.BigEndian.Uint32(header[0:4])
	if length < HeaderLen || length > MaxPDULength {
		return protocol.PDU{}, fmt.Errorf("invalid pdu length %d", length)
	}
	raw := make([]byte, length)
	copy(raw, header)
	if _, err := io.ReadFull(r, raw[HeaderLen:]); err != nil {
		return protocol.PDU{}, err
	}
	return Decode(raw)
}

func WritePDU(w io.Writer, p protocol.PDU) error {
	b, err := Encode(p)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func SplitTLV(body []byte, offset int) (mandatory []byte, tlvs []protocol.TLV, err error) {
	if offset < 0 || offset > len(body) {
		return nil, nil, fmt.Errorf("invalid tlv offset %d", offset)
	}
	mandatory = body[:offset]
	r := NewReader(body[offset:])
	for r.Rem() > 0 {
		if r.Rem() < 4 {
			return nil, nil, errors.New("truncated tlv header")
		}
		tag, _ := r.U16()
		l, _ := r.U16()
		v, e := r.Bytes(int(l))
		if e != nil {
			return nil, nil, e
		}
		tv := append([]byte(nil), v...)
		tlvs = append(tlvs, protocol.TLV{Tag: protocol.TLVTag(tag), Value: tv})
	}
	return mandatory, tlvs, nil
}
