package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Define the list of commodities and companies to fetch data for
	symbols := []string{"XAUUSD", "XAGUSD", "CL.1", "ZW.1", "AAPL", "GOOGL", "MSFT"}

	// Define the Kafka topic to write the data to
	topic := "finnhub-stream"

	// Configure the Kafka producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	// Define the Finnhub API endpoint and parameters
	endpoint := "https://finnhub.io/api/v1/quote"
	params := fmt.Sprintf("?symbol=%s&token=cgm7u01r01qlbmq7l5g0cgm7u01r01qlbmq7l5gg", symbols[0])

	// Loop over the symbols and fetch real-time data every 5 seconds
	for {
		for _, symbol := range symbols {
			// Update the API parameters with the current symbol
			params = fmt.Sprintf("?symbol=%s&token=<insert_your_finnhub_token_here>", symbol)

			// Send an HTTP GET request to the Finnhub API and decode the JSON response
			resp, err := http.Get(endpoint + params)
			if err != nil {
				fmt.Println(err)
				continue
			}
			defer resp.Body.Close()

			var data map[string]interface{}
			err = json.NewDecoder(resp.Body).Decode(&data)
			if err != nil {
				fmt.Println(err)
				continue
			}

			// Write the data to Kafka
			value, err := json.Marshal(data)
			if err != nil {
				fmt.Println(err)
				continue
			}

			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          value,
			}, nil)

			if err != nil {
				fmt.Println(err)
			}
		}

		// Wait for 5 seconds before fetching data again
		time.Sleep(5 * time.Second)
	}
}
