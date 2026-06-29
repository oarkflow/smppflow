package intelligence

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	phone "github.com/oarkflow/phone"
)

// NumberInfo is the normalized phone intelligence record used by routing,
// account policy, sender policy, DLR correlation and reporting.
type NumberInfo struct {
	Original      string            `json:"original"`
	RegionHint    string            `json:"region_hint,omitempty"`
	E164          string            `json:"e164"`
	National      string            `json:"national,omitempty"`
	International string            `json:"international,omitempty"`
	RFC3966       string            `json:"rfc3966,omitempty"`
	Country       string            `json:"country,omitempty"`
	CountryCode   string            `json:"country_code,omitempty"`
	CallingCode   string            `json:"calling_code,omitempty"`
	MCC           string            `json:"mcc,omitempty"`
	MNC           string            `json:"mnc,omitempty"`
	NNC           string            `json:"nnc,omitempty"` // some providers expose national/network node code as NNC
	NDC           string            `json:"ndc,omitempty"` // national destination code, if supplied by metadata
	Network       string            `json:"network,omitempty"`
	Carrier       string            `json:"carrier,omitempty"`
	Type          string            `json:"type,omitempty"`
	LineType      string            `json:"line_type,omitempty"`
	Portable      bool              `json:"portable,omitempty"`
	Possible      bool              `json:"possible"`
	Valid         bool              `json:"valid"`
	Blacklisted   bool              `json:"blacklisted,omitempty"`
	Whitelisted   bool              `json:"whitelisted,omitempty"`
	Meta          map[string]string `json:"meta,omitempty"`
}

type PrefixEntry struct {
	Prefix      string
	Country     string
	CountryCode string
	CallingCode string
	MCC         string
	MNC         string
	NNC         string
	NDC         string
	Network     string
	Carrier     string
	Type        string
	LineType    string
}

// Enricher can call HLR, MNP, carrier, anti-fraud or internal network services
// after github.com/oarkflow/phone has produced the canonical base record.
type Enricher interface {
	Enrich(NumberInfo) (NumberInfo, error)
}

type EnricherFunc func(NumberInfo) (NumberInfo, error)

func (f EnricherFunc) Enrich(info NumberInfo) (NumberInfo, error) { return f(info) }

type Resolver struct {
	entries     []PrefixEntry
	blacklist   map[string]struct{}
	whitelist   map[string]struct{}
	defaultISO2 string
	enrichers   []Enricher
}

func NewResolver(entries []PrefixEntry) *Resolver {
	return NewResolverWithRegion(entries, "")
}

func NewResolverWithRegion(entries []PrefixEntry, defaultISO2 string) *Resolver {
	return &Resolver{entries: entries, blacklist: map[string]struct{}{}, whitelist: map[string]struct{}{}, defaultISO2: strings.ToUpper(strings.TrimSpace(defaultISO2))}
}

func (r *Resolver) AddEnricher(e Enricher) {
	if e != nil {
		r.enrichers = append(r.enrichers, e)
	}
}

// Normalize returns a conservative E.164-like digit normalization. Prefer
// NormalizeWithRegion or LookupWithRegion for country-aware parsing.
func Normalize(s string) string {
	var b strings.Builder
	for i, rr := range s {
		if rr == '+' && i == 0 {
			b.WriteRune(rr)
			continue
		}
		if unicode.IsDigit(rr) {
			b.WriteRune(rr)
		}
	}
	out := b.String()
	if out != "" && out[0] != '+' {
		out = "+" + out
	}
	return out
}

func NormalizeWithRegion(s, region string) string {
	info := Verify(s, region)
	if info.E164 != "" {
		return info.E164
	}
	return Normalize(s)
}

func Verify(s string, region ...string) NumberInfo {
	reg := ""
	if len(region) > 0 {
		reg = strings.ToUpper(strings.TrimSpace(region[0]))
	}
	info := NumberInfo{Original: s, RegionHint: reg, Meta: map[string]string{}}

	// Use the enriched convenience API requested by the platform owner.
	verified := phone.Verify(s)
	mergePhoneValue(&info, verified)

	// Region-aware parse improves national input such as "6502530000" with "US".
	if reg != "" {
		if num, err := phone.Parse(s, reg); err == nil {
			info.E164 = phone.Format(num, phone.E164)
			info.National = phone.Format(num, phone.NATIONAL)
			info.International = phone.Format(num, phone.INTERNATIONAL)
			info.RFC3966 = phone.Format(num, phone.RFC3966)
			info.Valid = true
			info.Possible = true
		}
	}
	if info.E164 == "" {
		info.E164 = Normalize(s)
	}
	if info.International == "" && strings.HasPrefix(info.E164, "+") {
		info.International = info.E164
	}
	if info.Meta != nil && len(info.Meta) == 0 {
		info.Meta = nil
	}
	return info
}

func (r *Resolver) Lookup(s string) NumberInfo {
	return r.LookupWithRegion(s, r.defaultISO2)
}

func (r *Resolver) LookupWithRegion(s, region string) NumberInfo {
	info := Verify(s, region)
	if info.RegionHint == "" {
		info.RegionHint = r.defaultISO2
	}
	r.applyPrefix(&info)
	if _, ok := r.blacklist[info.E164]; ok {
		info.Blacklisted = true
		info.Valid = false
	}
	if len(r.whitelist) > 0 {
		_, ok := r.whitelist[info.E164]
		info.Whitelisted = ok
		info.Valid = ok
	}
	for _, enricher := range r.enrichers {
		out, err := enricher.Enrich(info)
		if err == nil {
			info = out
		}
	}
	return info
}

func (r *Resolver) applyPrefix(info *NumberInfo) {
	clean := strings.TrimPrefix(info.E164, "+")
	best := PrefixEntry{}
	bestLen := -1
	for _, e := range r.entries {
		p := strings.TrimPrefix(e.Prefix, "+")
		if p != "" && strings.HasPrefix(clean, p) && len(p) > bestLen {
			best = e
			bestLen = len(p)
		}
	}
	if bestLen < 0 {
		return
	}
	if info.Country == "" {
		info.Country = best.Country
	}
	if info.CountryCode == "" {
		info.CountryCode = best.CountryCode
	}
	if info.CallingCode == "" {
		info.CallingCode = best.CallingCode
	}
	if info.MCC == "" {
		info.MCC = best.MCC
	}
	if info.MNC == "" {
		info.MNC = best.MNC
	}
	if info.NNC == "" {
		info.NNC = best.NNC
	}
	if info.NDC == "" {
		info.NDC = best.NDC
	}
	if info.Network == "" {
		info.Network = best.Network
	}
	if info.Carrier == "" {
		info.Carrier = best.Carrier
	}
	if info.Type == "" {
		info.Type = best.Type
	}
	if info.LineType == "" {
		info.LineType = best.LineType
	}
}

func (r *Resolver) Blacklist(n string) {
	r.blacklist[NormalizeWithRegion(n, r.defaultISO2)] = struct{}{}
}
func (r *Resolver) Whitelist(n string) {
	r.whitelist[NormalizeWithRegion(n, r.defaultISO2)] = struct{}{}
}

func mergePhoneValue(info *NumberInfo, value any) {
	if value == nil {
		return
	}
	v := reflect.ValueOf(value)
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" { // unexported
			continue
		}
		name := strings.ToLower(f.Name)
		val := fmt.Sprint(v.Field(i).Interface())
		if val == "" || val == "<nil>" {
			continue
		}
		switch name {
		case "e164", "e164number", "internationalnumber", "number":
			if strings.HasPrefix(val, "+") {
				info.E164 = val
			}
		case "national", "nationalformat":
			info.National = val
		case "international", "internationalformat":
			info.International = val
		case "rfc3966":
			info.RFC3966 = val
		case "country", "countryname":
			info.Country = val
		case "region", "countrycode", "iso2", "regioncode":
			info.CountryCode = strings.ToUpper(val)
		case "callingcode", "countrycallingcode", "dialcode":
			info.CallingCode = val
		case "mcc":
			info.MCC = val
		case "mnc":
			info.MNC = val
		case "nnc":
			info.NNC = val
		case "ndc", "nationaldestinationcode":
			info.NDC = val
		case "network":
			info.Network = val
		case "carrier", "operator":
			info.Carrier = val
		case "type":
			info.Type = val
		case "linetype", "phonetype":
			info.LineType = val
		case "valid", "isvalid":
			info.Valid = boolValue(v.Field(i), val)
		case "possible", "ispossible":
			info.Possible = boolValue(v.Field(i), val)
		case "portable", "isportable":
			info.Portable = boolValue(v.Field(i), val)
		default:
			if info.Meta == nil {
				info.Meta = map[string]string{}
			}
			info.Meta[f.Name] = val
		}
	}
}

func boolValue(v reflect.Value, s string) bool {
	if v.Kind() == reflect.Bool {
		return v.Bool()
	}
	b, _ := strconv.ParseBool(strings.ToLower(strings.TrimSpace(s)))
	return b
}
