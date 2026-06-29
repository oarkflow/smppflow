package lab

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os/exec"
	"time"

	"github.com/oarkflow/smppflow/protocol"
)

type Fixture struct {
	Name         string                 `json:"name"`
	Operation    protocol.CommandID     `json:"operation"`
	Hex          string                 `json:"hex"`
	ExpectStatus protocol.CommandStatus `json:"expect_status"`
	Malformed    bool                   `json:"malformed"`
	Notes        string                 `json:"notes"`
}
type ReplayResult struct {
	Fixture  string
	Passed   bool
	Error    string
	Duration time.Duration
}
type PDUHandler interface {
	Handle(context.Context, []byte) ([]byte, error)
}

func SaveFixtures(w io.Writer, xs []Fixture) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(xs)
}
func LoadFixtures(r io.Reader) ([]Fixture, error) {
	var xs []Fixture
	err := json.NewDecoder(r).Decode(&xs)
	return xs, err
}
func Replay(ctx context.Context, h PDUHandler, fixtures []Fixture) []ReplayResult {
	out := make([]ReplayResult, 0, len(fixtures))
	for _, f := range fixtures {
		start := time.Now()
		raw, err := hex.DecodeString(f.Hex)
		if err == nil {
			_, err = h.Handle(ctx, raw)
		}
		passed := (err == nil && !f.Malformed) || (err != nil && f.Malformed)
		msg := ""
		if err != nil {
			msg = err.Error()
		}
		out = append(out, ReplayResult{Fixture: f.Name, Passed: passed, Error: msg, Duration: time.Since(start)})
	}
	return out
}
func FuzzCorpus(seed []byte, n int) [][]byte {
	if n <= 0 {
		n = 1
	}
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		b := append([]byte(nil), seed...)
		for j := 0; j < len(b)/8+1; j++ {
			if len(b) == 0 {
				break
			}
			idx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(b))))
			val := make([]byte, 1)
			_, _ = rand.Read(val)
			b[idx.Int64()] = val[0]
		}
		out = append(out, b)
	}
	return out
}
func RunTCPDump(ctx context.Context, iface, outfile string, port int) error {
	if iface == "" || outfile == "" || port <= 0 {
		return errors.New("iface outfile and port required")
	}
	cmd := exec.CommandContext(ctx, "tcpdump", "-i", iface, "-w", outfile, "tcp port", fmt.Sprint(port))
	return cmd.Run()
}

type SoakConfig struct {
	TPS         int
	Duration    time.Duration
	Concurrency int
}
type SoakResult struct {
	Sent     int64
	Failed   int64
	Duration time.Duration
	RPS      float64
}
type SoakSender interface {
	Send(context.Context, int) error
}

func Soak(ctx context.Context, cfg SoakConfig, s SoakSender) SoakResult {
	if cfg.TPS <= 0 {
		cfg.TPS = 100
	}
	if cfg.Duration <= 0 {
		cfg.Duration = time.Second
	}
	start := time.Now()
	deadline := time.NewTimer(cfg.Duration)
	defer deadline.Stop()
	tick := time.NewTicker(time.Second / time.Duration(cfg.TPS))
	defer tick.Stop()
	var res SoakResult
	seq := 0
	for {
		select {
		case <-ctx.Done():
			res.Duration = time.Since(start)
			return finish(res)
		case <-deadline.C:
			res.Duration = time.Since(start)
			return finish(res)
		case <-tick.C:
			seq++
			res.Sent++
			if err := s.Send(ctx, seq); err != nil {
				res.Failed++
			}
		}
	}
}
func finish(r SoakResult) SoakResult {
	if r.Duration > 0 {
		r.RPS = float64(r.Sent) / r.Duration.Seconds()
	}
	return r
}
