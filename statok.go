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

func Event(metricName string, value int, labels ...string) {
	if globalClient != nil {
		globalClient.Event(metricName, value, labels...)
	}
}

func EventWithError(metricName string, value int, labels ...string) error {
	if globalClient != nil {
		return globalClient.EventWithError(metricName, value, labels...)
	} else {
		return nil
	}
}

func EventValue(metricName string, value float64, labels ...string) {
	if globalClient != nil {
		globalClient.EventValue(metricName, value, labels...)
	}
}

func EventValueWithError(metricName string, value float64, labels ...string) error {
	if globalClient != nil {
		return globalClient.EventValueWithError(metricName, value, labels...)
	} else {
		return nil
	}
}
