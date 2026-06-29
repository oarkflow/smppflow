package intelligence

import "testing"

func TestResolverUsesPhoneAndPrefixMetadata(t *testing.T) {
	r := NewResolverWithRegion([]PrefixEntry{{Prefix: "+1650", Country: "United States", CountryCode: "US", CallingCode: "1", MCC: "310", MNC: "260", NNC: "650", NDC: "650", Network: "US mobile", Carrier: "T-Mobile", Type: "mobile", LineType: "mobile"}}, "US")
	info := r.LookupWithRegion("6502530000", "US")
	if !info.Valid || info.E164 != "+16502530000" || info.CountryCode != "US" || info.MNC != "260" || info.NNC != "650" || info.Network == "" {
		t.Fatalf("unexpected %+v", info)
	}
}

func TestResolverBlacklistWhitelist(t *testing.T) {
	r := NewResolver([]PrefixEntry{{Prefix: "+97798", Country: "Nepal", CountryCode: "NP", MCC: "429", Carrier: "Nepal Telecom", Type: "mobile"}})
	info := r.Lookup("+9779841000000")
	if !info.Valid || info.Carrier != "Nepal Telecom" {
		t.Fatalf("unexpected %+v", info)
	}
	r.Blacklist("+9779841000000")
	if r.Lookup("+9779841000000").Valid {
		t.Fatal("expected blacklisted invalid")
	}
}

func TestResolverEnricher(t *testing.T) {
	r := NewResolver(nil)
	r.AddEnricher(EnricherFunc(func(info NumberInfo) (NumberInfo, error) {
		info.MCC = "429"
		info.MNC = "01"
		info.Network = "HLR verified"
		info.Portable = true
		return info, nil
	}))
	info := r.Lookup("+9779841000000")
	if info.MCC != "429" || !info.Portable || info.Network != "HLR verified" {
		t.Fatalf("enrichment failed: %+v", info)
	}
}
