package mapreduce

import (
	"bufio"
	_ "bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	_ "strings"
	"sync"
	_ "syscall"
	"time"
)

func mapper(f os.FileInfo, ch chan map[string]int) {

	// lets open the file...
	fh, err := os.Open("data/" + f.Name())
	defer func() {
		if fh != nil {
			fh.Close()
		}
	}()
	if err != nil {
		log.Fatal("we go the error", err.Error())
	}
	m := make(map[string]int)
	s := bufio.NewScanner(fh)
	for s.Scan() {
		tokens := strings.Fields(s.Text())
		for _, t := range tokens {
			m[t]++
		}
	}

	ch <- m
	close(ch)
}

func shuffler(in []chan map[string]int, out []chan<- map[string]int) {
	var wg sync.WaitGroup
	wg.Add(len(in))
	for _, ch := range in {
		go func(c <-chan map[string]int) {
			for m := range c {
				for k, v := range m {
					if len(k)%2 == 0 {
						tmp := make(map[string]int)
						tmp[k] = v
						out[0] <- tmp
					} else {

						tmp := make(map[string]int)
						tmp[k] = v
						out[1] <- tmp
					}
				}
			}
			wg.Done()
		}(ch)
	}
	go func() {
		wg.Wait()
		close(out[0])
		close(out[1])
	}()
}

func reducer(in chan map[string]int, out chan<- map[string]int) {
	m := make(map[string]int)
	for x := range in {
		for k, v := range x {
			m[k] += v
		}
	}
	out <- m
	close(out)
}

func formatOutput(in []<-chan map[string]int) {
	var wg sync.WaitGroup
	wg.Add(len(in))

	//fmt.Println("\n The output here" , len(in))

	for i := 0; i < len(in); i++ {
		go func(c <-chan map[string]int) {
			for m := range c {
				for k, v := range m {
					fmt.Println("\n final", k, "::", v)
				}
			}
			wg.Done()
		}(in[i])
	}
	wg.Wait()
}

func Map_reduce_main() {

	files, err := ioutil.ReadDir("data")
	if err != nil {
		log.Println("unable to open the directory.")
		panic(fmt.Sprintf("unable to open the directory %v", err.Error()))
	}

	startTime := time.Now()
	var mapperChannels = make([]chan map[string]int, len(files))
	// lets build the map
	for id, f := range files {
		ch := make(chan map[string]int, 10)
		mapperChannels[id] = ch
		go mapper(f, ch)
	}
	// we have build the
	reduceEven := make(chan map[string]int, 100)
	reduceOdd := make(chan map[string]int, 100)

	go shuffler(mapperChannels, []chan<- map[string]int{reduceEven, reduceOdd})

	out1 := make(chan map[string]int, 100)
	out2 := make(chan map[string]int, 100)

	go reducer(reduceEven, out1)
	go reducer(reduceOdd, out2)

	formatOutput([]<-chan map[string]int{out1, out2})

	fmt.Println("Total time taken:", time.Now().Sub(startTime))

}
