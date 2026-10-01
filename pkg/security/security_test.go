package security_test

import (
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/security"
)

func TestHashSecretIsDeterministicAndNonReversible(t *testing.T) {
	h1 := security.HashSecret("s3cret")
	h2 := security.HashSecret("s3cret")
	if h1 != h2 {
		t.Fatal("HashSecret must be deterministic")
	}
	if h1 == "s3cret" {
		t.Fatal("HashSecret must not return the plaintext")
	}
	if security.HashSecret("other") == h1 {
		t.Fatal("distinct secrets must hash distinctly")
	}
	if len(h1) != 64 {
		t.Fatalf("sha256 hex length=%d want 64", len(h1))
	}
}

func TestConstantTimeEqualSecret(t *testing.T) {
	hash := security.HashSecret("s3cret")
	if !security.ConstantTimeEqualSecret("s3cret", hash) {
		t.Fatal("matching secret should verify")
	}
	if security.ConstantTimeEqualSecret("wrong", hash) {
		t.Fatal("wrong secret must not verify")
	}
	if security.ConstantTimeEqualSecret("", hash) {
		t.Fatal("empty secret must not verify")
	}
}

func TestEqualString(t *testing.T) {
	if !security.EqualString("abc", "abc") {
		t.Fatal("equal strings should match")
	}
	if security.EqualString("abc", "abd") {
		t.Fatal("differing strings must not match")
	}
	if security.EqualString("abc", "abcd") {
		t.Fatal("length mismatch must not match")
	}
	if security.EqualString("", "") != true {
		t.Fatal("empty strings are equal")
	}
}

func TestIPPolicyCompileErrors(t *testing.T) {
	// Malformed entries must fail Compile. Silently dropping them would leave
	// the allowlist empty and permit every address.
	cases := []struct {
		name string
		p    security.IPPolicy
	}{
		{"garbage ip", security.IPPolicy{AllowCIDR: []string{"not-an-ip"}}},
		{"garbage cidr", security.IPPolicy{AllowCIDR: []string{"10.0.0.0/99"}}},
		{"bad deny entry", security.IPPolicy{DenyCIDR: []string{"nope"}}},
		{"mixed valid and invalid", security.IPPolicy{AllowCIDR: []string{"10.0.0.0/8", "oops"}}},
	}
	for _, c := range cases {
		if err := c.p.Compile(); err == nil {
			t.Errorf("%s: Compile should fail", c.name)
		}
	}
}

func TestIPPolicyRejectsInvalidEntriesFailClosed(t *testing.T) {
	p := security.IPPolicy{AllowCIDR: []string{"typo-here"}}
	if err := p.Compile(); err == nil {
		t.Fatal("a malformed allowlist entry must be an error, not an empty (allow-all) list")
	}
}

func TestIPPolicyBlankEntriesAreIgnored(t *testing.T) {
	p := security.IPPolicy{AllowCIDR: []string{"", "  ", "10.0.0.0/8"}}
	if err := p.Compile(); err != nil {
		t.Fatalf("blank entries should be tolerated: %v", err)
	}
	if !p.Allowed("10.1.2.3:1") {
		t.Fatal("valid entry should still be honoured")
	}
}

func TestIPPolicyAllowsByDefault(t *testing.T) {
	p := &security.IPPolicy{}
	if err := p.Compile(); err != nil {
		t.Fatal(err)
	}
	if !p.Allowed("203.0.113.5:1234") {
		t.Fatal("empty policy should allow")
	}
}

func TestIPPolicyAllowList(t *testing.T) {
	p := &security.IPPolicy{AllowCIDR: []string{"10.0.0.0/8"}}
	if err := p.Compile(); err != nil {
		t.Fatal(err)
	}
	if !p.Allowed("10.1.2.3:9999") {
		t.Fatal("in-range IP should be allowed")
	}
	if p.Allowed("203.0.113.5:9999") {
		t.Fatal("out-of-range IP should be denied")
	}
}

func TestIPPolicyDenyWins(t *testing.T) {
	p := &security.IPPolicy{AllowCIDR: []string{"10.0.0.0/8"}, DenyCIDR: []string{"10.1.0.0/16"}}
	if err := p.Compile(); err != nil {
		t.Fatal(err)
	}
	if p.Allowed("10.1.2.3:1") {
		t.Fatal("deny must take precedence over allow")
	}
	if !p.Allowed("10.2.2.3:1") {
		t.Fatal("allowed IP outside deny range should pass")
	}
}

func TestIPPolicyAcceptsHostPortAndBareIP(t *testing.T) {
	p := &security.IPPolicy{AllowCIDR: []string{"192.0.2.1"}}
	if err := p.Compile(); err != nil {
		t.Fatal(err)
	}
	if !p.Allowed("192.0.2.1:8080") {
		t.Fatal("host:port form should be accepted")
	}
	if !p.Allowed("192.0.2.1") {
		t.Fatal("bare IP form should be accepted")
	}
	if p.Allowed("192.0.2.2") {
		t.Fatal("non-matching IP should be denied")
	}
}

func TestIPPolicyRejectsGarbageRemote(t *testing.T) {
	p := &security.IPPolicy{}
	if err := p.Compile(); err != nil {
		t.Fatal(err)
	}
	if p.Allowed("not-an-ip") {
		t.Fatal("unparseable remote must be denied")
	}
}

func TestIPPolicyIPv6(t *testing.T) {
	p := &security.IPPolicy{AllowCIDR: []string{"2001:db8::/32"}}
	if err := p.Compile(); err != nil {
		t.Fatal(err)
	}
	if !p.Allowed("[2001:db8::1]:443") {
		t.Fatal("IPv6 in-range should be allowed")
	}
	if p.Allowed("[2001:db9::1]:443") {
		t.Fatal("IPv6 out-of-range should be denied")
	}
}

func TestLimiterAllowsBurstThenThrottles(t *testing.T) {
	l := security.NewLimiter(1000, 3)
	allowed := 0
	for i := 0; i < 5; i++ {
		if l.Allow() {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("burst allowed=%d want 3", allowed)
	}
}

func TestLimiterRefillsOverTime(t *testing.T) {
	l := security.NewLimiter(1000, 1)
	if !l.Allow() {
		t.Fatal("first call must pass")
	}
	if l.Allow() {
		t.Fatal("second immediate call must be throttled")
	}
	time.Sleep(5 * time.Millisecond)
	if !l.Allow() {
		t.Fatal("token should refill after 5ms at 1000/s")
	}
}

func TestLimiterNormalisesBadConfig(t *testing.T) {
	l := security.NewLimiter(0, 0)
	// rate and burst fall back to 1; the first call must still succeed.
	if !l.Allow() {
		t.Fatal("limiter with zero config should still allow one call")
	}
	if l.Allow() {
		t.Fatal("burst of 1 must throttle the second call")
	}
}

func TestRedactorMasksAllOccurrences(t *testing.T) {
	r := security.Redactor{Secrets: []string{"hunter2", "token-abc"}}
	got := r.Redact("password=hunter2 and token-abc and hunter2")
	if strings.Contains(got, "hunter2") || strings.Contains(got, "token-abc") {
		t.Fatalf("secret leaked: %q", got)
	}
	if strings.Count(got, "***") != 3 {
		t.Fatalf("expected 3 masks, got %q", got)
	}
}

func TestRedactorIgnoresEmptySecrets(t *testing.T) {
	r := security.Redactor{Secrets: []string{"", "x"}}
	got := r.Redact("a x b")
	if got != "a *** b" {
		t.Fatalf("got %q", got)
	}
}

func TestRedactorDefaultMask(t *testing.T) {
	r := security.Redactor{Secrets: []string{"s"}}
	if got := r.Redact("s"); got != "***" {
		t.Fatalf("got %q want ***", got)
	}
}
