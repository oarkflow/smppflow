package lab

import (
	"context"
	"errors"
	"testing"
)

type handler func(context.Context, []byte) ([]byte, error)

func (h handler) Handle(ctx context.Context, b []byte) ([]byte, error) { return h(ctx, b) }

func TestReplayMalformed(t *testing.T) {
	res := Replay(context.Background(), handler(func(context.Context, []byte) ([]byte, error) { return nil, errors.New("bad") }), []Fixture{{Name: "bad", Hex: "01", Malformed: true}})
	if len(res) != 1 || !res[0].Passed {
		t.Fatalf("bad result %+v", res)
	}
}
