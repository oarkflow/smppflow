package codec

import (
	"encoding/binary"
	"errors"
)

type Writer struct{ B []byte }

func NewWriter(capacity int) *Writer         { return &Writer{B: make([]byte, 0, capacity)} }
func (w *Writer) U8(v byte)                  { w.B = append(w.B, v) }
func (w *Writer) U16(v uint16)               { w.B = append(w.B, byte(v>>8), byte(v)) }
func (w *Writer) U32(v uint32)               { w.B = append(w.B, byte(v>>24), byte(v>>16), byte(v>>8), byte(v)) }
func (w *Writer) CString(s string)           { w.B = append(w.B, s...); w.B = append(w.B, 0) }
func (w *Writer) Bytes(b []byte)             { w.B = append(w.B, b...) }
func (w *Writer) LenU8Bytes(b []byte)        { w.U8(byte(len(b))); w.Bytes(b) }
func (w *Writer) PatchU32(pos int, v uint32) { binary.BigEndian.PutUint32(w.B[pos:pos+4], v) }

type Reader struct {
	B []byte
	I int
}

func NewReader(b []byte) *Reader { return &Reader{B: b} }
func (r *Reader) Rem() int       { return len(r.B) - r.I }
func (r *Reader) U8() (byte, error) {
	if r.Rem() < 1 {
		return 0, errors.New("short u8")
	}
	v := r.B[r.I]
	r.I++
	return v, nil
}
func (r *Reader) U16() (uint16, error) {
	if r.Rem() < 2 {
		return 0, errors.New("short u16")
	}
	v := binary.BigEndian.Uint16(r.B[r.I:])
	r.I += 2
	return v, nil
}
func (r *Reader) U32() (uint32, error) {
	if r.Rem() < 4 {
		return 0, errors.New("short u32")
	}
	v := binary.BigEndian.Uint32(r.B[r.I:])
	r.I += 4
	return v, nil
}
func (r *Reader) CString() (string, error) {
	start := r.I
	for r.I < len(r.B) && r.B[r.I] != 0 {
		r.I++
	}
	if r.I >= len(r.B) {
		return "", errors.New("unterminated cstring")
	}
	s := string(r.B[start:r.I])
	r.I++
	return s, nil
}
func (r *Reader) Bytes(n int) ([]byte, error) {
	if r.Rem() < n {
		return nil, errors.New("short bytes")
	}
	b := r.B[r.I : r.I+n]
	r.I += n
	return b, nil
}
func (r *Reader) LenU8Bytes() ([]byte, error) {
	n, err := r.U8()
	if err != nil {
		return nil, err
	}
	return r.Bytes(int(n))
}
