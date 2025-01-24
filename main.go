package main

import (
	"encoding/json"
	"fmt"
	"goelasticapp/dao"
	"goelasticapp/utils"
	"log"
	"net/http"
	"strconv"
	"time"
)

func main() {
    // Create a new Elasticsearch client
    es, err := dao.NewESClient()
    if err != nil {
        log.Fatalf("Failed to create Elasticsearch client: %v", err)
    }

    // Example 1: Get all flights
    fmt.Println("1. Getting all flights:")
    results, err := es.GetAllFlights()
    if err != nil {
        log.Printf("Error getting all flights: %v", err)
    }
    utils.PrintResults(results)

    // Example 2: Get flights by carrier
    fmt.Println("\n2. Getting flights by carrier (ES-Air):")
    carrierResults, err := es.GetFlightsByCarrier("ES-Air")
    if err != nil {
        log.Printf("Error getting flights by carrier: %v", err)
    }
    utils.PrintResults(carrierResults)

    // Example 3: Get flights by price range
    fmt.Println("\n3. Getting flights in price range ($200-$400):")
    priceResults, err := es.GetFlightsByPriceRange(200, 400)
    if err != nil {
        log.Printf("Error getting flights by price range: %v", err)
    }
    utils.PrintResults(priceResults)

    // Example 4: Get flights by origin city
    fmt.Println("\n4. Getting flights from Adelaide:")
    originResults, err := es.GetFlightsByOriginCity("Adelaide")
    if err != nil {
        log.Printf("Error getting flights by origin: %v", err)
    }
    utils.PrintResults(originResults)

    // Example 5: Get long distance flights
    fmt.Println("\n5. Getting long distance flights (>5000km):")
    distanceResults, err := es.GetLongDistanceFlights(5000)
    if err != nil {
        log.Printf("Error getting long distance flights: %v", err)
    }
    utils.PrintResults(distanceResults)

    // Example 6: Get flights by date range
    startDate := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
    endDate := time.Date(2022, 1, 31, 0, 0, 0, 0, time.UTC)
    fmt.Println("\n6. Getting flights within date range:")
    dateResults, err := es.GetFlightsByDateRange(startDate, endDate)
    if err != nil {
        log.Printf("Error getting flights by date range: %v", err)
    }
    utils.PrintResults(dateResults)

    // Example 7: Get average price per carrier
    fmt.Println("\n7. Getting average price per carrier:")
    avgPriceResults, err := es.GetAveragePricePerCarrier()
    if err != nil {
        log.Printf("Error getting average price per carrier: %v", err)
    }
    utils.PrintAggregationResults(avgPriceResults)

    // Example 8: Get flights per destination
    fmt.Println("\n8. Getting flights per destination:")
    destResults, err := es.GetFlightsPerDestination()
    if err != nil {
        log.Printf("Error getting flights per destination: %v", err)
    }
    utils.PrintAggregationResults(destResults)

    // Example 9: Get delayed flights
    fmt.Println("\n9. Getting delayed flights (>60 minutes):")

	http.HandleFunc("/delayed-flights", DelayedFlightController)


    delayedResults, err := es.GetDelayedFlights(60)
    if err != nil {
        log.Printf("Error getting delayed flights: %v", err)
    }
    utils.PrintAggregationResults(delayedResults)

    // Example 10: Get flights by multiple criteria
    fmt.Println("\n10. Getting flights with multiple criteria:")
    multiResults, err := es.GetFlightsByMultipleCriteria("ES-Air", "Adelaide", "Tokoname")
    if err != nil {
        log.Printf("Error getting flights by multiple criteria: %v", err)
    }
    utils.PrintResults(multiResults)

	log.Println("Server is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// DelayedFlightController handles the delayed-flights endpoint
func DelayedFlightController(w http.ResponseWriter, r *http.Request) {
	es, err := dao.NewESClient()
    if err != nil {
        log.Fatalf("Failed to create Elasticsearch client: %v", err)
    }
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	delayTime := r.URL.Query().Get("time")
	if delayTime == "" {
		http.Error(w, "Missing query parameter 'time'", http.StatusBadRequest)
		return
	}

	// Call to GetDelayedFlights function, assume it returns a map and error
	delayTimeInt, err := strconv.Atoi(delayTime)
	if err != nil {
		http.Error(w, "Invalid 'time' query parameter, must be an integer", http.StatusBadRequest)
		return
	}

	delayedResults, err := es.GetDelayedFlights(delayTimeInt)
	if err != nil {
		log.Printf("Error getting delayed flights: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Set the content type to application/json
	w.Header().Set("Content-Type", "application/json")

	// Encode the delayedResults map into JSON and write to the response
	err = json.NewEncoder(w).Encode(delayedResults)
	if err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}
