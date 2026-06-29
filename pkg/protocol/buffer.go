package protocol

import "errors"

type writer struct{ B []byte }

func newWriter(capacity int) *writer  { return &writer{B: make([]byte, 0, capacity)} }
func (w *writer) U8(v byte)           { w.B = append(w.B, v) }
func (w *writer) U16(v uint16)        { w.B = append(w.B, byte(v>>8), byte(v)) }
func (w *writer) CString(s string)    { w.B = append(w.B, s...); w.B = append(w.B, 0) }
func (w *writer) Bytes(b []byte)      { w.B = append(w.B, b...) }
func (w *writer) LenU8Bytes(b []byte) { w.U8(byte(len(b))); w.Bytes(b) }

type reader struct {
	B []byte
	I int
}

func newReader(b []byte) *reader { return &reader{B: b} }
func (r *reader) Rem() int       { return len(r.B) - r.I }
func (r *reader) U8() (byte, error) {
	if r.Rem() < 1 {
		return 0, errors.New("short u8")
	}
	v := r.B[r.I]
	r.I++
	return v, nil
}
func (r *reader) U16() (uint16, error) {
	if r.Rem() < 2 {
		return 0, errors.New("short u16")
	}
	v := uint16(r.B[r.I])<<8 | uint16(r.B[r.I+1])
	r.I += 2
	return v, nil
}
func (r *reader) CString() (string, error) {
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
func (r *reader) Bytes(n int) ([]byte, error) {
	if r.Rem() < n {
		return nil, errors.New("short bytes")
	}
	b := r.B[r.I : r.I+n]
	r.I += n
	return b, nil
}
func (r *reader) LenU8Bytes() ([]byte, error) {
	n, err := r.U8()
	if err != nil {
		return nil, err
	}
	return r.Bytes(int(n))
}
