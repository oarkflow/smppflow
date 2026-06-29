package main

import (
	"fmt"

	"github.com/oarkflow/smppflow/intelligence"
)

func main() {
	resolver := intelligence.NewResolverWithRegion([]intelligence.PrefixEntry{
		{Prefix: "+1650", Country: "United States", CountryCode: "US", CallingCode: "1", MCC: "310", MNC: "260", NNC: "650", NDC: "650", Network: "US mobile", Carrier: "T-Mobile", Type: "mobile", LineType: "mobile"},
		{Prefix: "+97798", Country: "Nepal", CountryCode: "NP", CallingCode: "977", MCC: "429", MNC: "01", NNC: "98", NDC: "98", Network: "Nepal mobile", Carrier: "Nepal Telecom", Type: "mobile", LineType: "mobile"},
	}, "")

	resolver.AddEnricher(intelligence.EnricherFunc(func(info intelligence.NumberInfo) (intelligence.NumberInfo, error) {
		// Replace this hook with HLR/MNP/carrier/vendor lookup in production.
		if info.CountryCode == "US" {
			info.Portable = true
			if info.Meta == nil {
				info.Meta = map[string]string{}
			}
			info.Meta["lookup_source"] = "phone+prefix+enricher"
		}
		return info, nil
	}))

	fmt.Printf("US national input: %+v\n", resolver.LookupWithRegion("6502530000", "US"))
	fmt.Printf("Nepal E164 input:   %+v\n", resolver.LookupWithRegion("+9779841000000", "NP"))
}
