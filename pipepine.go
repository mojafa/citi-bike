package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const (
	apiKey   = "cgm7u01r01qlbmq7l5g0cgm7u01r01qlbmq7l5gg"
	baseURL  = "https://finnhub.io/api/v1"
	startStr = "2023-01-01"
	endStr   = "2023-04-22"
)

type Quote struct {
	Close  float64 `json:"c"`
	High   float64 `json:"h"`
	Low    float64 `json:"l"`
	Open   float64 `json:"o"`
	Time   int64   `json:"t"`
	Volume int64   `json:"v"`
	Symbol string  `json:"symbol"`
}

func main() {
	startTime, err := time.Parse("2006-01-02", startStr)
	if err != nil {
		panic(err)
	}
	endTime, err := time.Parse("2006-01-02", endStr)
	if err != nil {
		panic(err)
	}

	us30Quotes, err := getQuotes("index/constituents", "US30", startTime, endTime)
	if err != nil {
		panic(err)
	}
	fmt.Printf("US30 quotes between %s and %s:\n", startStr, endStr)
	for _, q := range us30Quotes {
		fmt.Printf("%d: open=%.2f, high=%.2f, low=%.2f, close=%.2f, volume=%d\n", q.Time, q.Open, q.High, q.Low, q.Close, q.Volume)
	}

	xauusdQuotes, err := getQuotes("forex/candle", "XAUUSD", startTime, endTime)
	if err != nil {
		panic(err)
	}
	fmt.Printf("XAUUSD quotes between %s and %s:\n", startStr, endStr)
	for _, q := range xauusdQuotes {
		fmt.Printf("%d: open=%.2f, high=%.2f, low=%.2f, close=%.2f, volume=%d\n", q.Time, q.Open, q.High, q.Low, q.Close, q.Volume)
	}
}

func getQuotes(endpoint, symbol string, startTime, endTime time.Time) ([]Quote, error) {
	url := fmt.Sprintf("%s/%s", baseURL, endpoint)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Set("symbol", symbol)
	q.Set("resolution", "D")
	q.Set("from", fmt.Sprintf("%d", startTime.Unix()))
	q.Set("to", fmt.Sprintf("%d", endTime.Unix()))
	q.Set("token", apiKey)
	req.URL.RawQuery = q.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data struct {
		Quotes []Quote `json:"c"`
	}
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return nil, err
	}
	return data.Quotes, nil
}
