package gostatok

import (
	"bytes"
	"errors"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/gammazero/deque"
	"github.com/golang/snappy"
	"github.com/statxyz/statok-go/pb"
	"google.golang.org/protobuf/proto"
)

var ErrDroppedEvent = errors.New("event dropped, too frequent events or send queue is full")

const clientVer = 1

const sendInterval = 1 * time.Second
const flushInterval = 1 * time.Second

const (
	metricTypeCounter = 0
	metricTypeValue   = 1
)

type counterType uint32
type valueType float32

type accum struct {
	labels  []string
	counter counterType
	values  []valueType
}

var accumsPool = sync.Pool{New: func() any {
	return make([]accum, 0, 10)
}}

var accumsValuesPool = sync.Pool{New: func() any {
	return make([]valueType, 0, 10)
}}

type eventEntry struct {
	metricName string
	metricType int
	labels     []string
	value      valueType
	counter    counterType
}

type Client struct {
	apiKey     string
	httpClient HTTPClient
	endpoint   string

	eventsChan chan eventEntry
	accumsMx   sync.Mutex
	accumsMap  [2]map[string][]accum

	sendQueue   deque.Deque[[]byte]
	sendQueueMx sync.Mutex
}

type Options struct {
	APIKey     string
	HTTPClient HTTPClient
	Endpoint   string
}

func NewClient(options Options) *Client {
	if options.HTTPClient == nil {
		options.HTTPClient = &http.Client{}
	}

	c := &Client{
		apiKey:     options.APIKey,
		httpClient: options.HTTPClient,
		accumsMap:  [2]map[string][]accum{make(map[string][]accum), make(map[string][]accum)},
		eventsChan: make(chan eventEntry, 2000),
	}

	if options.Endpoint != "" {
		c.endpoint = options.Endpoint
	} else {
		c.endpoint = "https://statok.dev0101.xyz"
	}

	go c.startEventsCollector()
	go c.startSerializer()
	go c.startSender()

	return c
}

func (c *Client) Event(metricName string, value counterType, labels ...string) {
	_ = c.EventWithError(metricName, value, labels...)
}

func (c *Client) EventWithError(metricName string, value counterType, labels ...string) error {
	c.eventsChan <- eventEntry{metricName, metricTypeCounter, labels, 0, value}
	return nil
}

func (c *Client) EventValue(metricName string, value valueType, labels ...string) {
	_ = c.EventValueWithError(metricName, value, labels...)
}

func (c *Client) EventValueWithError(metricName string, value valueType, labels ...string) error {
	c.eventsChan <- eventEntry{metricName, metricTypeValue, labels, value, 0}
	return nil
}

func (c *Client) startEventsCollector() {
	for entry := range c.eventsChan {
		func() {
			c.accumsMx.Lock()
			defer c.accumsMx.Unlock()

			accums := c.accumsMap[entry.metricType][entry.metricName]
			if accums == nil {
				accums = accumsPool.Get().([]accum)
				c.accumsMap[entry.metricType][entry.metricName] = accums
			}

			var acc *accum
			for ai, a := range accums {
				if len(a.labels) != len(entry.labels) {
					continue
				}
				isLabelsMatch := true
				for i := 0; i < len(a.labels); i++ {
					if a.labels[i] != entry.labels[i] {
						isLabelsMatch = false
						break
					}
				}

				if isLabelsMatch {
					acc = &accums[ai]
					break
				}
			}

			if acc == nil {
				accums = append(accums, accum{labels: entry.labels})
				acc = &accums[len(accums)-1]
				c.accumsMap[entry.metricType][entry.metricName] = accums
			}

			if entry.metricType == metricTypeCounter {
				acc.counter += entry.counter
			} else {
				if acc.values == nil {
					acc.values = accumsValuesPool.Get().([]valueType)
				}
				acc.counter += 1
				acc.values = append(acc.values, round(entry.value, 100))
			}
		}()
	}
}

func (c *Client) startSerializer() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for range ticker.C {
		func() {
			c.accumsMx.Lock()
			defer c.accumsMx.Unlock()

			if len(c.accumsMap[metricTypeCounter]) == 0 && len(c.accumsMap[metricTypeValue]) == 0 {
				return
			}

			metrics := &pb.Metrics{}
			metrics.ClientVersion = clientVer

			for _, metricType := range []int{metricTypeCounter, metricTypeValue} {
				for name, accums := range c.accumsMap[metricType] {
					metric := &pb.Metric{
						Name: name,
						Type: pb.MetricType(metricType),
					}

					for _, a := range accums {
						pbAccum := pb.Accum{
							Labels: a.labels,
							Count:  uint32(a.counter),
							Values: valuesToFloatSlice(a.values),
						}
						metric.Accums = append(metric.Accums, &pbAccum)
					}
					metrics.Metrics = append(metrics.Metrics, metric)
				}
			}

			data, err := proto.Marshal(metrics)
			if err != nil {
				return
			}

			for _, metricType := range []int{metricTypeCounter, metricTypeValue} {
				for _, accums := range c.accumsMap[metricType] {
					var zeroAccum accum
					for ai, a := range accums {
						if a.values != nil {
							accumsValuesPool.Put(a.values[0:0])
						}
						accums[ai] = zeroAccum
					}
					accumsPool.Put(accums[0:0])
				}
				clear(c.accumsMap[metricType])
			}

			c.sendQueueMx.Lock()
			defer c.sendQueueMx.Unlock()
			c.sendQueue.PushBack(data)
		}()
	}
}

func (c *Client) startSender() {
	ticker := time.NewTicker(sendInterval)
	defer ticker.Stop()

	for range ticker.C {
		func() {
			c.sendQueueMx.Lock()
			if c.sendQueue.Len() == 0 {
				c.sendQueueMx.Unlock()
				return
			}

			data := c.sendQueue.Front()
			c.sendQueueMx.Unlock()

			err := c.sendToAPI(data)
			if err == nil {
				c.sendQueueMx.Lock()
				c.sendQueue.PopFront()
				c.sendQueueMx.Unlock()
			}
		}()
	}
}

var snappyCompressBufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (c *Client) sendToAPI(data []byte) error {
	var buf *bytes.Buffer

	buf = snappyCompressBufferPool.Get().(*bytes.Buffer)
	defer snappyCompressBufferPool.Put(buf)
	buf.Reset()

	writer := snappy.NewBufferedWriter(buf)
	_, _ = writer.Write(data)
	_ = writer.Close()

	req, err := http.NewRequest("POST", c.endpoint+"/api/i", buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Encoding", "snappy")

	resp, err := c.httpClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return err
	}
	_ = resp.Body.Close()

	return nil
}

func valuesToFloatSlice(values []valueType) []float32 {
	result := make([]float32, len(values))
	for i, v := range values {
		result[i] = float32(v)
	}
	return result
}

func round[T ~float64 | ~float32, V int](v T, precision V) T {
	return T(math.Round(float64(v)*float64(precision)) / float64(precision))
}
