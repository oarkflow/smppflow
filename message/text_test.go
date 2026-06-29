package message

import "testing"

func TestAnalyzeAndSegmentUCS2(t *testing.T) {
	segs, a, err := SegmentText("hello 👋 world", SegmentPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if a.Encoding != EncodingUCS2 {
		t.Fatalf("want ucs2 got %s", a.Encoding)
	}
	if len(segs) != 1 {
		t.Fatalf("want 1 seg got %d", len(segs))
	}
}
func TestLongMultipartSAR(t *testing.T) {
	txt := ""
	for i := 0; i < 400; i++ {
		txt += "a"
	}
	segs, _, err := SegmentText(txt, SegmentPolicy{UseSAR: true, Ref: 9})
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 2 {
		t.Fatal("expected multipart")
	}
	if len(segs[0].TLVs) != 3 {
		t.Fatalf("expected sar tlvs")
	}
	if segs[0].ESMClass != 0 {
		t.Fatalf("SAR must not set UDH esm class")
	}
}
func TestInvisibleDetection(t *testing.T) {
	a := AnalyzeText("a\u2060b")
	if !a.ContainsInvisible || len(a.Invisible) != 1 {
		t.Fatalf("expected invisible: %+v", a)
	}
}
