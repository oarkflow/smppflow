package protocol

import "strings"

type AddressRole string

const (
	AddressRoleAny         AddressRole = "any"
	AddressRoleSource      AddressRole = "source"
	AddressRoleDestination AddressRole = "destination"
	AddressRoleESME        AddressRole = "esme"
)

type AddressFormat string

const (
	AddressFormatEmpty         AddressFormat = "empty"
	AddressFormatE164          AddressFormat = "e164"
	AddressFormatInternational AddressFormat = "international"
	AddressFormatNational      AddressFormat = "national"
	AddressFormatSubscriber    AddressFormat = "subscriber"
	AddressFormatShortCode     AddressFormat = "short_code"
	AddressFormatAlphanumeric  AddressFormat = "alphanumeric"
	AddressFormatInternet      AddressFormat = "internet"
	AddressFormatUnknown       AddressFormat = "unknown"
)

type AddressAnalysis struct {
	Input      string
	Normalized string
	Format     AddressFormat
	Role       AddressRole
	TON        TON
	NPI        NPI
	Reason     string
}

func (t TON) String() string {
	switch t {
	case TONUnknown:
		return "unknown"
	case TONInternational:
		return "international"
	case TONNational:
		return "national"
	case TONNetwork:
		return "network_specific"
	case TONSubscriber:
		return "subscriber"
	case TONAlphanumeric:
		return "alphanumeric"
	case TONAbbreviated:
		return "abbreviated"
	default:
		return "ton_" + itoaByte(byte(t))
	}
}

func (n NPI) String() string {
	switch n {
	case NPIUnknown:
		return "unknown"
	case NPIISDN:
		return "isdn_e164"
	case NPIData:
		return "data_x121"
	case NPITelex:
		return "telex"
	case NPILandMob:
		return "land_mobile"
	case NPINational:
		return "national"
	case NPIPrivate:
		return "private"
	case NPIERMES:
		return "ermes"
	case NPIInternet:
		return "internet"
	case NPIWAP:
		return "wap"
	default:
		return "npi_" + itoaByte(byte(n))
	}
}

func itoaByte(v byte) string {
	if v == 0 {
		return "0"
	}
	var b [3]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = '0' + v%10
		v /= 10
	}
	return string(b[i:])
}

// DetectAddress analyzes an SMPP source/destination address and returns the
// normalized address bytes together with the best TON/NPI pair for SMPP PDUs.
// It is deliberately allocation-light and deterministic; no external metadata
// or network lookups are required.
func DetectAddress(raw string) AddressAnalysis {
	in := strings.TrimSpace(raw)
	low := strings.ToLower(in)
	if strings.HasPrefix(low, "tel:") || strings.HasPrefix(low, "sms:") {
		in = strings.TrimSpace(in[4:])
		low = strings.ToLower(in)
	}
	if in == "" {
		return AddressAnalysis{Input: raw, Normalized: "", Format: AddressFormatEmpty, TON: TONUnknown, NPI: NPIUnknown, Reason: "empty address"}
	}
	if strings.Contains(in, "@") {
		return AddressAnalysis{Input: raw, Normalized: in, Format: AddressFormatInternet, TON: TONUnknown, NPI: NPIInternet, Reason: "internet style address"}
	}
	in = stripVisualSeparators(in)
	if isAlphaNumericSender(in) {
		return AddressAnalysis{Input: raw, Normalized: in, Format: AddressFormatAlphanumeric, TON: TONAlphanumeric, NPI: NPIUnknown, Reason: "contains alphabetic sender-id characters"}
	}
	if strings.HasPrefix(in, "+") && allDigits(in[1:]) {
		digits := in[1:]
		return AddressAnalysis{Input: raw, Normalized: digits, Format: AddressFormatE164, TON: TONInternational, NPI: NPIISDN, Reason: "leading plus E.164 address"}
	}
	if strings.HasPrefix(in, "00") && len(in) > 4 && allDigits(in[2:]) {
		digits := in[2:]
		return AddressAnalysis{Input: raw, Normalized: digits, Format: AddressFormatInternational, TON: TONInternational, NPI: NPIISDN, Reason: "international 00 prefix"}
	}
	if allDigits(in) {
		l := len(in)
		switch {
		case l <= 6:
			return AddressAnalysis{Input: raw, Normalized: in, Format: AddressFormatShortCode, TON: TONAbbreviated, NPI: NPIUnknown, Reason: "short numeric code"}
		case strings.HasPrefix(in, "0"):
			return AddressAnalysis{Input: raw, Normalized: in, Format: AddressFormatNational, TON: TONNational, NPI: NPIISDN, Reason: "national trunk prefix"}
		case l == 7:
			return AddressAnalysis{Input: raw, Normalized: in, Format: AddressFormatSubscriber, TON: TONSubscriber, NPI: NPIISDN, Reason: "subscriber length numeric address"}
		case l >= 8 && l <= 15:
			return AddressAnalysis{Input: raw, Normalized: in, Format: AddressFormatInternational, TON: TONInternational, NPI: NPIISDN, Reason: "international numeric address"}
		default:
			return AddressAnalysis{Input: raw, Normalized: in, Format: AddressFormatUnknown, TON: TONUnknown, NPI: NPIUnknown, Reason: "numeric address outside common SMPP length bounds"}
		}
	}
	return AddressAnalysis{Input: raw, Normalized: in, Format: AddressFormatUnknown, TON: TONUnknown, NPI: NPIUnknown, Reason: "unrecognized address format"}
}

// AutoDetectAddress fills TON/NPI from address format when they are left as
// unknown/zero. Explicit non-zero TON/NPI values are preserved while still
// normalizing common address decorations such as +, spaces, dashes and tel:.
func AutoDetectAddress(a Address) Address {
	analysis := DetectAddress(a.Addr)
	a.Addr = analysis.Normalized
	if a.TON == TONUnknown {
		a.TON = analysis.TON
	}
	if a.NPI == NPIUnknown {
		a.NPI = analysis.NPI
	}
	return a
}

func stripVisualSeparators(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case ' ', '\t', '\n', '\r', '-', '.', '(', ')':
			continue
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func isAlphaNumericSender(s string) bool {
	if len(s) > 20 {
		return false
	}
	hasLetter := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z':
			hasLetter = true
		case c >= 'a' && c <= 'z':
			hasLetter = true
		case c >= '0' && c <= '9':
		case c == '_' || c == '-' || c == '.':
		default:
			return false
		}
	}
	return hasLetter
}
