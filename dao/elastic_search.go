package dao

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "os"
    "time"
    elasticsearch "github.com/elastic/go-elasticsearch/v8"
    "github.com/joho/godotenv"
)

type ESClient struct {
    client *elasticsearch.Client
}

// NewESClient creates a new Elasticsearch client
func NewESClient() (*ESClient, error) {
    err := godotenv.Load()
    if err != nil {
        return nil, fmt.Errorf("error loading .env file: %v", err)
    }

    apiKey := os.Getenv("ES_LOCAL_API_KEY")
    if apiKey == "" {
        return nil, fmt.Errorf("missing ES_LOCAL_API_KEY in environment variables")
    }

    cfg := elasticsearch.Config{
        Addresses: []string{"http://localhost:9200"},
        APIKey:    apiKey,
    }

    client, err := elasticsearch.NewClient(cfg)
    if err != nil {
        return nil, fmt.Errorf("error creating Elasticsearch client: %v", err)
    }

    return &ESClient{client: client}, nil
}

// executeSearch performs the search operation with the given query
func (es *ESClient) executeSearch(query map[string]interface{}) (map[string]interface{}, error) {
    var buf bytes.Buffer
    if err := json.NewEncoder(&buf).Encode(query); err != nil {
        return nil, fmt.Errorf("error encoding query: %v", err)
    }

    res, err := es.client.Search(
        es.client.Search.WithContext(context.Background()),
        es.client.Search.WithIndex("kibana_sample_data_flights"),
        es.client.Search.WithBody(&buf),
        es.client.Search.WithTrackTotalHits(true),
    )
    if err != nil {
        return nil, fmt.Errorf("error performing search: %v", err)
    }
    defer res.Body.Close()

    var result map[string]interface{}
    if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
        return nil, fmt.Errorf("error parsing response: %v", err)
    }

    return result, nil
}

// GetAllFlights retrieves all flights
func (es *ESClient) GetAllFlights() (map[string]interface{}, error) {
    query := map[string]interface{}{
        "query": map[string]interface{}{
            "match_all": map[string]interface{}{},
        },
    }
    return es.executeSearch(query)
}

// GetFlightsByCarrier retrieves flights by carrier
func (es *ESClient) GetFlightsByCarrier(carrier string) (map[string]interface{}, error) {
    query := map[string]interface{}{
        "query": map[string]interface{}{
            "term": map[string]interface{}{
                "Carrier": carrier,
            },
        },
    }
    return es.executeSearch(query)
}

// GetFlightsByOriginCity retrieves flights by origin city
func (es *ESClient) GetFlightsByOriginCity(city string) (map[string]interface{}, error) {
    query := map[string]interface{}{
        "query": map[string]interface{}{
            "match": map[string]interface{}{
                "OriginCityName": city,
            },
        },
    }
    return es.executeSearch(query)
}

// GetFlightsByPriceRange retrieves flights within a price range
func (es *ESClient) GetFlightsByPriceRange(minPrice, maxPrice float64) (map[string]interface{}, error) {
    query := map[string]interface{}{
        "query": map[string]interface{}{
            "range": map[string]interface{}{
                "AvgTicketPrice": map[string]interface{}{
                    "gte": minPrice,
                    "lte": maxPrice,
                },
            },
        },
    }
    return es.executeSearch(query)
}

// GetLongDistanceFlights retrieves flights over a certain distance
func (es *ESClient) GetLongDistanceFlights(minDistance float64) (map[string]interface{}, error) {
    query := map[string]interface{}{
        "query": map[string]interface{}{
            "range": map[string]interface{}{
                "DistanceKilometers": map[string]interface{}{
                    "gt": minDistance,
                },
            },
        },
    }
    return es.executeSearch(query)
}

// GetFlightsByDateRange retrieves flights within a date range
func (es *ESClient) GetFlightsByDateRange(startDate, endDate time.Time) (map[string]interface{}, error) {
    query := map[string]interface{}{
        "query": map[string]interface{}{
            "range": map[string]interface{}{
                "FlightDate": map[string]interface{}{
                    "gte": startDate.Format("2006-01-02"),
                    "lte": endDate.Format("2006-01-02"),
                },
            },
        },
    }
    return es.executeSearch(query)
}

// GetAveragePricePerCarrier retrieves average ticket price per carrier
func (es *ESClient) GetAveragePricePerCarrier() (map[string]interface{}, error) {
    query := map[string]interface{}{
        "size": 0,
        "aggs": map[string]interface{}{
            "avg_price_per_carrier": map[string]interface{}{
                "terms": map[string]interface{}{
                    "field": "Carrier.keyword",
                },
                "aggs": map[string]interface{}{
                    "average_price": map[string]interface{}{
                        "avg": map[string]interface{}{
                            "field": "AvgTicketPrice",
                        },
                    },
                },
            },
        },
    }
    return es.executeSearch(query)
}

// GetFlightsPerDestination retrieves total flights per destination country
func (es *ESClient) GetFlightsPerDestination() (map[string]interface{}, error) {
    query := map[string]interface{}{
        "size": 0,
        "aggs": map[string]interface{}{
            "flights_per_country": map[string]interface{}{
                "terms": map[string]interface{}{
                    "field": "DestCountry.keyword",
                },
            },
        },
    }
    return es.executeSearch(query)
}

// GetDelayedFlights retrieves delayed flights and calculates average delay
func (es *ESClient) GetDelayedFlights(minDelayMinutes int) (map[string]interface{}, error) {
    query := map[string]interface{}{
        "size": 0,
        "query": map[string]interface{}{
            "range": map[string]interface{}{
                "FlightDelayMin": map[string]interface{}{
                    "gt": minDelayMinutes,
                },
            },
        },
        "aggs": map[string]interface{}{
            "avg_delay_time": map[string]interface{}{
                "avg": map[string]interface{}{
                    "field": "FlightDelayMin",
                },
            },
        },
    }
    return es.executeSearch(query)
}

// GetFlightsByMultipleCriteria retrieves flights matching multiple criteria
func (es *ESClient) GetFlightsByMultipleCriteria(carrier, originCity, destCity string) (map[string]interface{}, error) {
    query := map[string]interface{}{
        "query": map[string]interface{}{
            "bool": map[string]interface{}{
                "must": []map[string]interface{}{
                    {
                        "term": map[string]interface{}{
                            "Carrier": carrier,
                        },
                    },
                    {
                        "match": map[string]interface{}{
                            "OriginCityName": originCity,
                        },
                    },
                    {
                        "match": map[string]interface{}{
                            "DestCityName": destCity,
                        },
                    },
                },
            },
        },
    }
    return es.executeSearch(query)
}
