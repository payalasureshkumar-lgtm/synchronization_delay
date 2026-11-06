package main

import (
	"fmt"
	"sync"
	"time"
)

const iterations = 1000

type Barrier struct {
	mu         sync.Mutex
	cond       *sync.Cond
	total      int
	arrived    int
	generation int
}

func NewBarrier(total int) *Barrier {
	b := &Barrier{total: total}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *Barrier) Wait() {
	b.mu.Lock()

	generation := b.generation
	b.arrived++

	if b.arrived == b.total {
		b.arrived = 0
		b.generation++
		b.cond.Broadcast()
	} else {
		for generation == b.generation {
			b.cond.Wait()
		}
	}

	b.mu.Unlock()
}

func runCluster(nodes int) float64 {
	barrier := NewBarrier(nodes)

	var wg sync.WaitGroup
	var totalDelay time.Duration
	var mu sync.Mutex

	for i := 0; i < nodes; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				start := time.Now()
				barrier.Wait()
				delay := time.Since(start)

				mu.Lock()
				totalDelay += delay
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	return float64(totalDelay.Nanoseconds()) /
		float64(nodes*iterations) / 1e6
}

func main() {
	clusterSizes := []int{3, 5, 7, 9, 11}

	fmt.Println("Cluster Size,Conventional Delay (ms)")

	for _, nodes := range clusterSizes {
		delay := runCluster(nodes)
		fmt.Printf("%d,%.6f\n", nodes, delay)
	}
}
