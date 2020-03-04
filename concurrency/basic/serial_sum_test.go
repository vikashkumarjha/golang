package basic

import "testing"

func BenchmarkSerialSum(b *testing.B) {
	for i := 0; i < b.N; i++ {
		SerialSum()
	}
}