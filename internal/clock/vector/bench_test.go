package vector_test

import (
	"testing"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
)

// BenchmarkCompare is the hot path used by the conflict detector and
// happens-before graph. Wall-clock target: < 200 ns/op for small clocks.
func BenchmarkCompare(b *testing.B) {
	a := vector.VectorClock{"P1": 1, "P2": 2, "P3": 3}
	c := vector.VectorClock{"P1": 1, "P2": 3, "P3": 2}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vector.Compare(a, c)
	}
}

func BenchmarkCompare_Large(b *testing.B) {
	pids := make([]string, 50)
	for i := range pids {
		pids[i] = "P" + string(rune('A'+i%26)) + string(rune('0'+i%10))
	}
	a := randVC(pids)
	c := randVC(pids)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vector.Compare(a, c)
	}
}

func BenchmarkTick(b *testing.B) {
	vc := vector.New([]string{"P1", "P2", "P3"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vc = vector.Tick(vc, "P1")
	}
}

func BenchmarkReceive(b *testing.B) {
	vc := vector.VectorClock{"P1": 0, "P2": 0, "P3": 0}
	incoming := vector.VectorClock{"P1": 5, "P2": 5, "P3": 5}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vc = vector.Receive(vc, incoming, "P1")
	}
}

func BenchmarkMergePassive(b *testing.B) {
	a := randVC([]string{"P1", "P2", "P3", "P4", "P5"})
	c := randVC([]string{"P1", "P2", "P3", "P4", "P5"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vector.MergePassive(a, c)
	}
}
