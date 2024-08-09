package approx

import (
	"github.com/statxyz/statok-go/commons"
	"math"
	"slices"
)

const valuesDigestMaxValuesBeforeApprox = 32
const valuesDigestResultRoundPrecision = 10

var Percentiles = [...]float32{0.50, 0.75, 0.95, 0.99}

const percentilesCount = len(Percentiles)

type ValuesApproxDigest struct {
	percentiles [percentilesCount]*Psqr

	min float32
	max float32

	// Kahan avg
	sum   float64
	c     float64 // Compensation for lost low-order bits.
	count uint32
}

func (vad *ValuesApproxDigest) Reset() {
	vad.min = math.MaxFloat32
	vad.max = 0
	vad.sum = 0
	vad.count = 0
	vad.c = 0

	for i, p := range Percentiles {
		if vad.percentiles[i] == nil {
			vad.percentiles[i] = NewPsqr(p)
		}
		vad.percentiles[i].Reset()
	}
}

func (vad *ValuesApproxDigest) Add(value float32) {
	for i := range vad.percentiles {
		vad.percentiles[i].Add(value)
	}

	vad.min = min(vad.min, value)
	vad.max = max(vad.max, value)

	y := float64(value) - vad.c
	t := vad.sum + y
	vad.c = (t - vad.sum) - y
	vad.sum = t
	vad.count++
}

func (vad *ValuesApproxDigest) Avg() float32 {
	if vad.count == 0 {
		return 0
	}
	return float32(round(vad.sum/float64(vad.count), valuesDigestResultRoundPrecision))
}

type ValuesDigest struct {
	values []float32
	approx *ValuesApproxDigest
}

var (
	valueDigestsPool = commons.NewPool(func() *ValuesDigest {
		return &ValuesDigest{}
	})
	valueApproxDigestsPool = commons.NewPool(func() *ValuesApproxDigest {
		return &ValuesApproxDigest{}
	})
	valueValuesDigestsPool = commons.NewPool(func() []float32 {
		return make([]float32, valuesDigestMaxValuesBeforeApprox)
	})
)

func ReleaseValueDigest(digest *ValuesDigest) {
	if digest != nil {
		digest.Reset()
		valueDigestsPool.Put(digest)
	}
}

func NewValuesDigest() *ValuesDigest {
	return valueDigestsPool.Get()
}

func newValuesApproxDigest() *ValuesApproxDigest {
	approx := valueApproxDigestsPool.Get()
	approx.Reset()
	return approx
}

func (vd *ValuesDigest) Reset() {
	valueValuesDigestsPool.Put(vd.values[:0])
	vd.values = nil
	if vd.approx != nil {
		valueApproxDigestsPool.Put(vd.approx)
		vd.approx = nil
	}
}

func (vd *ValuesDigest) Add(value float32) {
	if vd.approx != nil {
		vd.approx.Add(value)
	} else {
		if len(vd.values) >= valuesDigestMaxValuesBeforeApprox {
			vd.approx = newValuesApproxDigest()
			for _, v := range vd.values {
				vd.approx.Add(v)
			}
			valueValuesDigestsPool.Put(vd.values[:0])
			vd.values = nil
			vd.approx.Add(value)
		} else {
			if vd.values == nil {
				vd.values = valueValuesDigestsPool.Get()
			}
			vd.values = append(vd.values, value)
		}
	}
}

func (vd *ValuesDigest) Result(cb func(float32, int)) {
	if vd.approx == nil {
		slices.SortFunc(vd.values, func(a, b float32) int {
			if a < b {
				return -1
			} else {
				return 1
			}
		})

		var sum float64
		for _, v := range vd.values {
			sum += float64(v)
		}

		cb(float32(sum/float64(len(vd.values))), 0)
		cb(vd.values[0], 1)
		cb(vd.values[len(vd.values)-1], 2)
		for i, p := range Percentiles {
			cb(percentile(vd.values, p), i+3)
		}
	} else {
		cb(vd.approx.Avg(), 0)
		cb(vd.approx.min, 1)
		cb(vd.approx.max, 2)
		for pi, _ := range Percentiles {
			cb(vd.approx.percentiles[pi].Get(), pi+3)
		}
	}
}

func round[T ~float32 | ~float64, V float64 | int | uint32 | uint](v T, precision V) T {
	return T(math.Round(float64(v)*float64(precision)) / float64(precision))
}

func percentile(data []float32, p float32) float32 {
	k := p / 100 * float32(len(data)-1)
	f := int(k)
	c := f + 1
	if c > len(data)-1 {
		c = len(data) - 1
	}
	return data[f] + (data[c]-data[f])*(k-float32(f))
}
