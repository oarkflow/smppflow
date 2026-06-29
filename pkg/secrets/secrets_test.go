package secrets

import (
	"context"
	"testing"
)

func TestLocalKMSStoreRoundTripAndRotate(t *testing.T) {
	st := NewStore(NewLocalKMS([]byte("master")))
	if err := st.Put(context.Background(), "x", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	b, err := st.Get(context.Background(), "x")
	if err != nil || string(b) != "secret" {
		t.Fatalf("got %q err %v", b, err)
	}
	if err := st.Rotate(context.Background(), "x", []byte("next")); err != nil {
		t.Fatal(err)
	}
	b, _ = st.Get(context.Background(), "x")
	if string(b) != "next" {
		t.Fatalf("rotate failed: %q", b)
	}
}
