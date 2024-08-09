package gostatok

import (
	"net/http"
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

var globalClient *Client

func Init(options Options) {
	globalClient = NewClient(options)
}

func Event[T ~int | ~int8 | ~int16 | ~int32 | ~uint | ~uint8 | ~uint16 | ~uint32](metricName string, value T, labels ...string) {
	_ = EventWithError(metricName, max(0, value), labels...)
}

func EventWithError[T ~int | ~int8 | ~int16 | ~int32 | ~uint | ~uint8 | ~uint16 | ~uint32](metricName string, value T, labels ...string) error {
	if globalClient != nil {
		return globalClient.EventWithError(metricName, uint32(max(0, value)), labels...)
	} else {
		return nil
	}
}

func EventValue[T ~float32 | ~float64](metricName string, value T, labels ...string) {
	_ = EventValueWithError(metricName, value, labels...)
}

func EventValueWithError[T ~float32 | ~float64](metricName string, value T, labels ...string) error {
	if globalClient != nil {
		return globalClient.EventValueWithError(metricName, float32(value), labels...)
	} else {
		return nil
	}
}
