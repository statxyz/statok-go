package gostatok

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestSendEvents(t *testing.T) {
	Init(Options{APIKey: "Test"})

	for i := 0; i < 99; i++ {
		//now := time.Now()
		//math.Ceil(float64(now.Minute())/10)
		EventValue("test_metric_v"+strconv.Itoa(rand.Intn(2)), rand.NormFloat64(), "aaa_"+strconv.Itoa(rand.Intn(8)), "bbb_2"+strconv.Itoa(rand.Intn(4)))
	}

	time.Sleep(time.Minute * 9999)
}
