package main

type predicate func(int) bool


func filter(a[] int, f predicate) []int{
	res := []int{}
	for _, v := range a {
		if f(v) {
			res = append(res, v)
		}
	}
	return res

}

func isOdd(x int) bool {
	return x % 2 != 0
}

/*
func main() {
	fmt.Println(" We will write the filter method.")
	a := []int{ 1, 2, 34, 5 , 12 , 7}
	r := filter(a,isOdd)
	for _, v := range r {
		fmt.Println(" " , v)
	}

}
 */

