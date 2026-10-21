package poe

import "math"

// deterministicLog and deterministicExp are the explicit-rounding variants of
// the portable Go implementations in math.Log and math.Exp. The standard
// library selects hand-written assembly on arm64 and amd64; those routines are
// correct to the last bit for their respective platforms, but they are not
// required to return the same bits on both. PoE weights are persisted in the
// AppHash, so the consensus path needs a single architecture-independent
// result.
//
// The algorithms and constants are derived from Go's BSD-licensed
// src/math/log.go and src/math/exp.go. Every operation that can otherwise be
// fused into an architecture-dependent multiply-add is expressed with
// math.FMA, whose result is specified as one correctly-rounded fused operation
// on every architecture.
func deterministicLog(x float64) float64 {
	const (
		ln2Hi = 6.93147180369123816490e-01
		ln2Lo = 1.90821492927058770002e-10
		l1    = 6.666666666666735130e-01
		l2    = 3.999999999940941908e-01
		l3    = 2.857142874366239149e-01
		l4    = 2.222219843214978396e-01
		l5    = 1.818357216161805012e-01
		l6    = 1.531383769920937332e-01
		l7    = 1.479819860511658591e-01
	)

	switch {
	case math.IsNaN(x) || math.IsInf(x, 1):
		return x
	case x < 0:
		return math.NaN()
	case x == 0:
		return math.Inf(-1)
	}

	f1, ki := math.Frexp(x)
	if f1 < math.Sqrt2/2 {
		f1 *= 2
		ki--
	}
	f := f1 - 1
	k := float64(ki)

	s := f / (2 + f)
	s2 := s * s
	s4 := s2 * s2
	t1 := s2 * math.FMA(s4, math.FMA(s4, math.FMA(s4, l7, l5), l3), l1)
	t2 := s4 * math.FMA(s4, math.FMA(s4, l6, l4), l2)
	r := t1 + t2
	hfsq := 0.5 * f * f
	inner := math.FMA(s, hfsq+r, k*ln2Lo)
	return k*ln2Hi - ((hfsq - inner) - f)
}

func deterministicExp(x float64) float64 {
	const (
		ln2Hi     = 6.93147180369123816490e-01
		ln2Lo     = 1.90821492927058770002e-10
		log2e     = 1.44269504088896338700e+00
		overflow  = 7.09782712893383973096e+02
		underflow = -7.45133219101941108420e+02
		nearZero  = 1.0 / (1 << 28)
	)

	switch {
	case math.IsNaN(x):
		return x
	case x > overflow:
		return math.Inf(1)
	case x < underflow:
		return 0
	case -nearZero < x && x < nearZero:
		return 1 + x
	}

	var k int
	switch {
	case x < 0:
		k = int(math.FMA(log2e, x, -0.5))
	case x > 0:
		k = int(math.FMA(log2e, x, 0.5))
	}
	hi := math.FMA(-float64(k), ln2Hi, x)
	lo := float64(k) * ln2Lo
	return deterministicExpMulti(hi, lo, k)
}

func deterministicExpMulti(hi, lo float64, k int) float64 {
	const (
		p1 = 1.66666666666666657415e-01
		p2 = -2.77777777770155933842e-03
		p3 = 6.61375632143793436117e-05
		p4 = -1.65339022054652515390e-06
		p5 = 4.13813679705723846039e-08
	)

	r := hi - lo
	t := r * r
	poly := math.FMA(t, p5, p4)
	poly = math.FMA(t, poly, p3)
	poly = math.FMA(t, poly, p2)
	poly = math.FMA(t, poly, p1)
	c := math.FMA(-t, poly, r)
	rc := r * c
	fraction := rc / (2 - c)
	y := 1 - ((lo - fraction) - hi)
	return math.Ldexp(y, k)
}
