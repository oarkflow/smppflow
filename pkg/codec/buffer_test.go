package codec_test

import (
	"bytes"
	"testing"

	"github.com/oarkflow/smppflow/pkg/codec"
)

func TestWriterReaderRoundTrip(t *testing.T) {
	w := codec.NewWriter(16)
	w.U8(0xAB)
	w.U16(0x1234)
	w.U32(0xDEADBEEF)
	w.CString("hello")
	w.Bytes([]byte{1, 2, 3})
	w.LenU8Bytes([]byte{9, 8})

	want := []byte{
		0xAB,
		0x12, 0x34,
		0xDE, 0xAD, 0xBE, 0xEF,
		'h', 'e', 'l', 'l', 'o', 0,
		1, 2, 3,
		2, 9, 8,
	}
	if !bytes.Equal(w.B, want) {
		t.Fatalf("encoded=%x want %x", w.B, want)
	}

	r := codec.NewReader(w.B)
	if v, err := r.U8(); err != nil || v != 0xAB {
		t.Fatalf("U8=%x,%v", v, err)
	}
	if v, err := r.U16(); err != nil || v != 0x1234 {
		t.Fatalf("U16=%x,%v", v, err)
	}
	if v, err := r.U32(); err != nil || v != 0xDEADBEEF {
		t.Fatalf("U32=%x,%v", v, err)
	}
	if v, err := r.CString(); err != nil || v != "hello" {
		t.Fatalf("CString=%q,%v", v, err)
	}
	if v, err := r.Bytes(3); err != nil || !bytes.Equal(v, []byte{1, 2, 3}) {
		t.Fatalf("Bytes=%x,%v", v, err)
	}
	if v, err := r.LenU8Bytes(); err != nil || !bytes.Equal(v, []byte{9, 8}) {
		t.Fatalf("LenU8Bytes=%x,%v", v, err)
	}
	if r.Rem() != 0 {
		t.Fatalf("Rem=%d want 0", r.Rem())
	}
}

func TestWriterPatchU32(t *testing.T) {
	w := codec.NewWriter(8)
	w.U32(0)
	w.U32(0xFFFFFFFF)
	w.PatchU32(0, 0x01020304)
	want := []byte{0x01, 0x02, 0x03, 0x04, 0xFF, 0xFF, 0xFF, 0xFF}
	if !bytes.Equal(w.B, want) {
		t.Fatalf("patched=%x want %x", w.B, want)
	}
}

func TestWriterCStringEmpty(t *testing.T) {
	w := codec.NewWriter(2)
	w.CString("")
	if !bytes.Equal(w.B, []byte{0}) {
		t.Fatalf("empty cstring=%x want 00", w.B)
	}
	r := codec.NewReader(w.B)
	if v, err := r.CString(); err != nil || v != "" {
		t.Fatalf("CString=%q,%v", v, err)
	}
}

func TestReaderShortErrors(t *testing.T) {
	if _, err := codec.NewReader(nil).U8(); err == nil {
		t.Fatal("U8 on empty must fail")
	}
	if _, err := codec.NewReader([]byte{1}).U16(); err == nil {
		t.Fatal("U16 on 1 byte must fail")
	}
	if _, err := codec.NewReader([]byte{1, 2, 3}).U32(); err == nil {
		t.Fatal("U32 on 3 bytes must fail")
	}
	if _, err := codec.NewReader([]byte{'a', 'b'}).CString(); err == nil {
		t.Fatal("unterminated cstring must fail")
	}
	if _, err := codec.NewReader([]byte{1, 2}).Bytes(3); err == nil {
		t.Fatal("short Bytes must fail")
	}
	if _, err := codec.NewReader([]byte{5, 1}).LenU8Bytes(); err == nil {
		t.Fatal("short LenU8Bytes must fail")
	}
	if _, err := codec.NewReader(nil).LenU8Bytes(); err == nil {
		t.Fatal("LenU8Bytes without length byte must fail")
	}
}
