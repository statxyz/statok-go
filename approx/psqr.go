package approx

// Psqr collects observations and returns an estimate of requested p-quantile, as described in the P-Square algorithm
type Psqr struct {
	count int
	q     [5]float32
	perc  float32
	n     [5]int
	np    [5]float32
	dn    [5]float32
}

// NewPsqr returns a new instance of Psqr
func NewPsqr(q float32) *Psqr {
	p := &Psqr{}
	p.perc = q
	p.Reset()
	return p
}

func sign(f float32) int {
	if f < 0.0 {
		return -1
	}
	return 1
}

func parabolic(p *Psqr, i, d int) float32 {
	qi, qip1, qim1 := p.q[i], p.q[i+1], p.q[i-1]
	ni, nip1, nim1 := float32(p.n[i]), float32(p.n[i+1]), float32(p.n[i-1])
	df := float32(d)
	return qi + df/(nip1-nim1)*((ni-nim1+df)*(qip1-qi)/(nip1-ni)+(nip1-ni-df)*(qi-qim1)/(ni-nim1))
}

func linear(p *Psqr, i, d int) float32 {
	df := float32(d)
	return p.q[i] + df*(p.q[i+d]-p.q[i])/float32(p.n[i+d]-p.n[i])
}

// Add collects a new observation, updates marker positions and the current estimate
func (p *Psqr) Add(v float32) float32 {
	if p.count < 5 {
		// store the first observations
		p.q[p.count], p.count = v, p.count+1

		if p.count == 5 {
			// sort the first observations
			for i := 1; i < p.count; i++ {
				for j := i; j > 0 && p.q[j-1] > p.q[j]; j-- {
					p.q[j], p.q[j-1] = p.q[j-1], p.q[j]
				}
			}
		}

		// note that p.q[2] is meaningless at this point
		return p.q[2]
	}

	p.count = p.count + 1

	// find cell k such that [qk < xj < qk+1] and adjust extreme values if necessary
	var k int
	for k = 0; k < 5; k++ {
		if v < p.q[k] {
			break
		}
	}

	if k == 0 {
		k = 1
		p.q[0] = v
	} else if k == 5 {
		k = 4
		p.q[4] = v
	}

	// increment positions of markers k+1 through 5
	for i := k; i < 5; i++ {
		p.n[i]++
	}

	// update desired positions for all markers
	for i := 0; i < 5; i++ {
		p.np[i] = p.np[i] + p.dn[i]
	}

	// adjust heights of markers 2-4 if necessary
	for i := 1; i < 4; i++ {
		d := p.np[i] - float32(p.n[i])
		if (d >= 1.0 && p.n[i+1]-p.n[i] > 1) || (d <= -1.0 && p.n[i-1]-p.n[i] < -1) {
			ds := sign(d)
			qp := parabolic(p, i, ds)

			if p.q[i-1] < qp && qp < p.q[i+1] {
				p.q[i] = qp
			} else {
				p.q[i] = linear(p, i, ds)
			}
			p.n[i] = p.n[i] + ds
		}
	}

	// return the current estimate of p-quantile
	return p.q[2]
}

// Get returns the current estimate of p-quantile
func (p *Psqr) Get() float32 {
	return p.q[2]
}

func (p *Psqr) Reset() {
	q := p.perc

	p.count = 0

	// calculate and store the increment in desired marker positions
	p.dn[0], p.dn[1], p.dn[2], p.dn[3], p.dn[4] = 0.0, q*0.5, q, (1+q)*0.5, 1.0

	// set initial marker positions
	for i := 0; i < 5; i++ {
		p.n[i] = i + 1
		p.np[i] = p.dn[i]*4 + 1
	}
}
