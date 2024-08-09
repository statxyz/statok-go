package gostatok

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"github.com/statxyz/statok-go/approx"
	"github.com/statxyz/statok-go/commons"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrDroppedEvent = errors.New("event dropped")

const flushInterval = time.Millisecond * 333

type metric struct {
	name   string
	accums []accum
}

type Step uint16

const (
	Step10s   Step = 10
	Step60s        = 60
	Step600s       = 600
	Step3600s      = 3600
)

var Steps = [...]Step{Step10s, Step60s, Step600s, Step3600s}

const StepsCount = len(Steps)

type accum struct {
	timeIndex int
	step      Step
	labels    []string
	counter   uint32
	digest    *approx.ValuesDigest
}

func (a *accum) isReadyToSend() bool {
	nowIndex := TimeToTimeIndex(time.Now().Unix(), a.step)
	return a.timeIndex < nowIndex
}

var (
	metricsPool = commons.NewPool(func() *metric {
		return &metric{
			accums: make([]accum, 0, 5),
		}
	})
)

type eventEntry struct {
	metricName string
	labels     []string
	value      float32
	counter    uint32
	ts         int64
}

type Client struct {
	apiKey   string
	clientId int

	httpClient HTTPClient
	endpoint   string

	eventsChan      chan eventEntry
	metricAccumsMx  sync.Mutex
	metricAccumsMap map[string]*metric

	sendQueue chan *bytes.Buffer
}

type Options struct {
	APIKey     string
	HTTPClient HTTPClient
	Endpoint   string
}

//func NewClientWith(options Options) *Client {
//
//}

func NewClient(options Options) *Client {
	if options.HTTPClient == nil {
		options.HTTPClient = &http.Client{}
	}

	apiKeyParts := strings.Split(options.APIKey, "_")
	if len(apiKeyParts) != 2 {
		log.Fatalf("invalid api key: %s", options.APIKey)
	}
	clientId, _ := strconv.Atoi(apiKeyParts[0])

	c := &Client{
		apiKey:          options.APIKey,
		clientId:        clientId,
		httpClient:      options.HTTPClient,
		metricAccumsMap: make(map[string]*metric),
		eventsChan:      make(chan eventEntry, 10000),
		sendQueue:       make(chan *bytes.Buffer, 10),
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

func (c *Client) Event(metricName string, value uint32, labels ...string) {
	_ = c.EventWithError(metricName, value, labels...)
}

func (c *Client) EventWithError(metricName string, value uint32, labels ...string) error {
	if value == 0 {
		return nil
	}

	select {
	case c.eventsChan <- eventEntry{metricName, labels, 0, value, time.Now().Unix()}:
	default:
		return ErrDroppedEvent
	}
	return nil
}

func (c *Client) EventValue(metricName string, value float32, labels ...string) {
	_ = c.EventValueWithError(metricName, value, labels...)
}

func (c *Client) EventValueWithError(metricName string, value float32, labels ...string) error {
	select {
	case c.eventsChan <- eventEntry{metricName, labels, value, 0, time.Now().Unix()}:
	default:
		return ErrDroppedEvent
	}
	return nil
}

func (c *Client) startEventsCollector() {
	for entry := range c.eventsChan {
		func() {
			c.metricAccumsMx.Lock()
			defer c.metricAccumsMx.Unlock()

			m := c.metricAccumsMap[entry.metricName]
			if m == nil {
				m = metricsPool.Get()
				m.name = entry.metricName
				c.metricAccumsMap[entry.metricName] = m
			}

			for _, step := range Steps {
				if entry.counter != 0 {
					// If the counter metric, then there is no need to accumulate values for anything other than 10 seconds
					if step != Step10s {
						continue
					}
				}

				timeIndex := TimeToTimeIndex(entry.ts, step)

				var acc *accum
				for ai, a := range m.accums {
					if a.step != step || a.timeIndex != timeIndex {
						continue
					}
					if len(a.labels) != len(entry.labels) {
						continue
					}
					isLabelsMatch := true
					for i := range len(a.labels) {
						if a.labels[i] != entry.labels[i] {
							isLabelsMatch = false
							break
						}
					}

					if isLabelsMatch {
						acc = &m.accums[ai]
						break
					}
				}

				if acc == nil {
					m.accums = append(m.accums, accum{timeIndex, step, entry.labels, 0, nil})
					acc = &m.accums[len(m.accums)-1]
				}

				if entry.counter > 0 {
					acc.counter += entry.counter
				} else {
					acc.counter += 1
					if acc.digest == nil {
						acc.digest = approx.NewValuesDigest()
					}
					acc.digest.Add(entry.value)
				}
			}
		}()
	}
}

var bytesBufferPool = commons.NewPool(func() *bytes.Buffer {
	return &bytes.Buffer{}
})

func (c *Client) startSerializer() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for range ticker.C {
		serialized := func() *bytes.Buffer {
			c.metricAccumsMx.Lock()
			defer c.metricAccumsMx.Unlock()

			if len(c.metricAccumsMap) == 0 {
				return nil
			}

			bbTotal := bytesBufferPool.Get()
			bbTotal.Reset()

			// [LEN,CLIENT_ID,METRIC_NAME,[{s:60, t:999, l:["x","y","z"],c:222,v:[]}]]

			metricsSerializedCount := 0
			for name, m := range c.metricAccumsMap {
				bb := bytesBufferPool.Get()
				bb.Reset()

				bb.WriteString(`[`)

				ai := 0
				for _, a := range m.accums {
					if !a.isReadyToSend() {
						continue
					}

					if ai > 0 {
						bb.WriteString(`,`)
					}
					bb.WriteString(`{"t":`)

					bb.WriteString(strconv.Itoa(a.timeIndex))
					bb.WriteString(`,"s":`)

					bb.WriteString(strconv.Itoa(int(a.step)))
					bb.WriteString(`,`)

					if len(a.labels) > 0 {
						bb.WriteString(`"l":[`)
						for li, l := range a.labels {
							if li > 0 {
								bb.WriteString(`,`)
							}
							bb.WriteString(`"` + l + `"`)
						}
						bb.Cap()
						bb.WriteString(`],`)
					}

					bb.WriteString(`"c":`)
					bb.WriteString(strconv.Itoa(int(a.counter)))

					if a.digest != nil {
						bb.WriteString(`,"v":[`)
						a.digest.Result(func(f float32, i int) {
							if i > 0 {
								bb.WriteString(`,`)
							}
							_, frac := math.Modf(float64(f))
							if f > 999 || frac == 0.0 {
								bb.WriteString(strconv.Itoa(int(f)))
							} else {
								bb.WriteString(fmt.Sprintf("%.1f", f))
							}
						})
						bb.WriteString(`]`)
					}
					bb.WriteString(`}`)

					ai++
				}

				if ai == 0 {
					bytesBufferPool.Put(bb)
					continue
				}

				bb.WriteString(`]`)

				compressed := compressBuffer(bb)

				bbTotal.WriteString(strconv.Itoa(c.clientId))
				bbTotal.WriteString(",")
				bbTotal.WriteString(name)
				bbTotal.WriteString(",")
				bbTotal.WriteString(strconv.Itoa(compressed.Len()))
				bbTotal.WriteString(",")
				bbTotal.Write(compressed.Bytes())

				bytesBufferPool.Put(bb)

				metricsSerializedCount += 1
			}

			if metricsSerializedCount == 0 {
				bytesBufferPool.Put(bbTotal)
				return nil
			}

			for mName, m := range c.metricAccumsMap {
				keepIndex := 0
				for _, a := range m.accums {
					if a.isReadyToSend() {
						approx.ReleaseValueDigest(a.digest)
						a.digest = nil
					} else {
						m.accums[keepIndex] = a
						keepIndex++
					}
				}
				m.accums = m.accums[:keepIndex]

				if len(m.accums) == 0 {
					metricsPool.Put(m)
					delete(c.metricAccumsMap, mName)
				}
			}

			return bbTotal
		}()

		if serialized != nil {
			if serialized.Len() == 0 {
				bytesBufferPool.Put(serialized)
				continue
			}

			c.sendQueue <- serialized
		}
	}
}

func (c *Client) startSender() {
	for bb := range c.sendQueue {
		tries := 3
		for tries > 0 {
			err := c.sendToAPI(bb)
			if err == nil {
				break
			}
			tries--
			time.Sleep(5 * time.Second)
		}
		bytesBufferPool.Put(bb)
	}
}

func (c *Client) sendToAPI(data *bytes.Buffer) error {
	req, err := http.NewRequest("POST", c.endpoint+"/api/i2", bytes.NewReader(data.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

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

var encoder, _ = zstd.NewWriter(nil)
var encoderMx sync.Mutex

func compressBuffer(input *bytes.Buffer) *bytes.Buffer {
	encoderMx.Lock()
	defer encoderMx.Unlock()

	output := bytesBufferPool.Get()
	output.Reset()

	encoder.Reset(output)
	_, _ = encoder.Write(input.Bytes())
	_ = encoder.Close()

	return output
}

func TimeToTimeIndex[T int | int64 | uint, S Step | int](ts T, step S) int {
	return int(math.Floor(float64(ts) / float64(step)))
}
