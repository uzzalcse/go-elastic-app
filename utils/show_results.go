package utils

import (
	"fmt"		
)

// Helper function to print search results
func PrintResults(results map[string]interface{}) {
    if results == nil {
        fmt.Println("No results found")
        return
    }

    hits, ok := results["hits"].(map[string]interface{})
    if !ok {
        fmt.Println("Invalid results format")
        return
    }

    totalHits, ok := hits["total"].(map[string]interface{})
    if ok {
        fmt.Printf("Total hits: %v\n", totalHits["value"])
    }

    hitsArray, ok := hits["hits"].([]interface{})
    if !ok {
        fmt.Println("No hits found")
        return
    }

    for i, hit := range hitsArray {
        if i >= 3 {  // Print only first 3 results
            fmt.Println("...")
            break
        }
        
        hitMap, ok := hit.(map[string]interface{})
        if !ok {
            continue
        }
        
        source, ok := hitMap["_source"].(map[string]interface{})
        if !ok {
            continue
        }
        
        // Print relevant fields
        fmt.Printf("Flight: %v -> %v (Carrier: %v, Price: $%.2f)\n",
            source["OriginCityName"],
            source["DestCityName"],
            source["Carrier"],
            source["AvgTicketPrice"])
    }
}

// Helper function to print aggregation results
func PrintAggregationResults(results map[string]interface{}) {
    if results == nil {
        fmt.Println("No results found")
        return
    }

    aggs, ok := results["aggregations"].(map[string]interface{})
    if !ok {
        fmt.Println("No aggregations found")
        return
    }

    // Print based on aggregation type
    if avgPricePerCarrier, ok := aggs["avg_price_per_carrier"].(map[string]interface{}); ok {
        buckets, ok := avgPricePerCarrier["buckets"].([]interface{})
        if ok {
            for _, bucket := range buckets {
                b := bucket.(map[string]interface{})
                carrier := b["key"]
                avgPrice := b["average_price"].(map[string]interface{})["value"]
                fmt.Printf("Carrier: %v, Average Price: $%.2f\n", carrier, avgPrice)
            }
        }
    }

    if flightsPerCountry, ok := aggs["flights_per_country"].(map[string]interface{}); ok {
        buckets, ok := flightsPerCountry["buckets"].([]interface{})
        if ok {
            for _, bucket := range buckets {
                b := bucket.(map[string]interface{})
                country := b["key"]
                count := b["doc_count"]
                fmt.Printf("Country: %v, Flights: %v\n", country, count)
            }
        }
    }

    if avgDelayTime, ok := aggs["avg_delay_time"].(map[string]interface{}); ok {
        value := avgDelayTime["value"]
        fmt.Printf("Average Delay Time: %.2f minutes\n", value)
    }
}