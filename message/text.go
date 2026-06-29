package message

import (
	"encoding/binary"
	"fmt"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/oarkflow/smppflow/protocol"
)

type Encoding string

const (
	EncodingGSM7   Encoding = "gsm7"
	EncodingUCS2   Encoding = "ucs2"
	EncodingBinary Encoding = "binary"
)

type TextAnalysis struct {
	Encoding          Encoding
	Runes             int
	GSMSeptets        int
	SingleLimit       int
	MultipartLimit    int
	Parts             int
	DataCoding        byte
	ContainsUnicode   bool
	ContainsInvisible bool
	Invisible         []InvisibleChar
}
type InvisibleChar struct {
	Position int
	Rune     rune
	Code     string
}
type SegmentPolicy struct {
	UseSAR            bool
	UseMessagePayload bool
	Ref               uint16
}
type Segment struct {
	Index      int
	Total      int
	Ref        uint16
	DataCoding byte
	ESMClass   byte
	Payload    []byte
	TLVs       []protocol.TLV
}

var gsmBasic = map[rune]bool{}
var gsmExt = map[rune]bool{'^': true, '{': true, '}': true, '\\': true, '[': true, '~': true, ']': true, '|': true, '€': true}

func init() {
	for _, r := range "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ ÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà" {
		gsmBasic[r] = true
	}
}
func isInvisible(r rune) bool {
	return (r >= 0x200B && r <= 0x200F) || (r >= 0x202A && r <= 0x202E) || (r >= 0x2060 && r <= 0x206F) || r == 0xFEFF || r == 0x00AD || r == 0x034F || r == 0x061C || r == 0x180E || r == 0x205F
}

func AnalyzeText(s string) TextAnalysis {
	a := TextAnalysis{Encoding: EncodingGSM7, SingleLimit: 160, MultipartLimit: 153, DataCoding: protocol.DataCodingDefault}
	pos := 0
	for _, r := range s {
		a.Runes++
		if isInvisible(r) {
			a.ContainsInvisible = true
			a.Invisible = append(a.Invisible, InvisibleChar{Position: pos, Rune: r, Code: fmt.Sprintf("U+%04X", r)})
		}
		pos += utf8.RuneLen(r)
		if gsmBasic[r] {
			a.GSMSeptets++
		} else if gsmExt[r] {
			a.GSMSeptets += 2
		} else {
			a.Encoding = EncodingUCS2
			a.ContainsUnicode = true
			a.DataCoding = protocol.DataCodingUCS2
			a.SingleLimit = 70
			a.MultipartLimit = 67
		}
	}
	units := a.GSMSeptets
	if a.Encoding == EncodingUCS2 {
		units = len(utf16.Encode([]rune(s)))
	}
	if units == 0 {
		a.Parts = 1
	} else if units <= a.SingleLimit {
		a.Parts = 1
	} else {
		a.Parts = (units + a.MultipartLimit - 1) / a.MultipartLimit
	}
	return a
}

func EncodeText(s string, enc Encoding) ([]byte, byte, error) {
	if enc == EncodingBinary {
		return []byte(s), protocol.DataCodingBinary, nil
	}
	if enc == EncodingUCS2 {
		rs := utf16.Encode([]rune(s))
		out := make([]byte, len(rs)*2)
		for i, u := range rs {
			binary.BigEndian.PutUint16(out[i*2:], u)
		}
		return out, protocol.DataCodingUCS2, nil
	}
	// production compatibility: byte-per-septet GSM alphabet subset; suitable for SMPP providers that accept unpacked default alphabet.
	out := make([]byte, 0, len(s))
	for _, r := range s {
		if gsmBasic[r] || gsmExt[r] {
			if r < 128 {
				out = append(out, byte(r))
			} else {
				out = append(out, '?')
			}
		} else {
			return nil, 0, fmt.Errorf("rune %U requires UCS2", r)
		}
	}
	return out, protocol.DataCodingDefault, nil
}

func SegmentText(s string, pol SegmentPolicy) ([]Segment, TextAnalysis, error) {
	a := AnalyzeText(s)
	payload, dc, err := EncodeText(s, a.Encoding)
	if err != nil && a.Encoding == EncodingGSM7 {
		payload, dc, err = EncodeText(s, EncodingUCS2)
		a.Encoding = EncodingUCS2
		a.DataCoding = dc
		a.SingleLimit = 70
		a.MultipartLimit = 67
	}
	if err != nil {
		return nil, a, err
	}
	unitSize := 1
	if a.Encoding == EncodingUCS2 {
		unitSize = 2
	}
	limit := a.SingleLimit * unitSize
	multi := a.MultipartLimit * unitSize
	if len(payload) <= limit {
		return []Segment{{Index: 1, Total: 1, Ref: pol.Ref, DataCoding: dc, Payload: payload}}, a, nil
	}
	if pol.Ref == 0 {
		pol.Ref = uint16(len(payload)*31 + int(dc))
	}
	var segs []Segment
	for off := 0; off < len(payload); {
		end := off + multi
		if end > len(payload) {
			end = len(payload)
		}
		if unitSize == 2 && end < len(payload) && (end-off)%2 != 0 {
			end--
		}
		segs = append(segs, Segment{Index: len(segs) + 1, Ref: pol.Ref, DataCoding: dc, Payload: append([]byte(nil), payload[off:end]...)})
		off = end
	}
	for i := range segs {
		segs[i].Total = len(segs)
		if pol.UseSAR {
			segs[i].TLVs = []protocol.TLV{protocol.TLVUint16(protocol.TLVSARMsgRefNum, pol.Ref), protocol.TLVByte(protocol.TLVSARTotalSegments, byte(len(segs))), protocol.TLVByte(protocol.TLVSARSegmentSeqnum, byte(i+1))}
		} else {
			udh := []byte{0x05, 0x00, 0x03, byte(pol.Ref), byte(len(segs)), byte(i + 1)}
			segs[i].Payload = append(udh, segs[i].Payload...)
			segs[i].ESMClass = protocol.ESMClassUDHIndicator
		}
	}
	return segs, a, nil
}
