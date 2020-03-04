package basic

const (
	limit = 10000000000
)

func SerialSum() int {
	sum := 0
	for i := 0; i < limit; i++ {
		sum += i
	}
	return sum
}