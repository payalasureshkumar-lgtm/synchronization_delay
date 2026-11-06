package main

import (
	"fmt"
	"sync"
	"time"
)

const (
	iterations = 1000
)

type Barrier interface {
	Wait()
}

type ConventionalBarrier struct {
	mu        sync.Mutex
	cond      *sync.Cond
	total     int
	arrived   int
	generation int
}

func NewConventionalBarrier(total int) *ConventionalBarrier {
	b := &ConventionalBarrier{total: total}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *ConventionalBarrier) Wait() {
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

type IntelligentBarrier struct {
	total   int
	arrivals chan time.Time
	release  chan struct{}
}

func NewIntelligentBarrier(total int) *IntelligentBarrier {
	b := &IntelligentBarrier{
		total:    total,
		arrivals: make(chan time.Time, total),
		release:  make(chan struct{}),
	}

	go b.coordinate()
	return b
}

func (b *IntelligentBarrier) coordinate() {
	for {
		var lastArrival time.Time

		for i := 0; i < b.total; i++ {
			arrival := <-b.arrivals
			if arrival.After(lastArrival) {
				lastArrival = arrival
			}
		}

		close(b.release)

		b.release = make(chan struct{})
	}
}

func (b *IntelligentBarrier) Wait() {
	release := b.release
	b.arrivals <- time.Now()
	<-release
}

func measureConventional(nodes int) float64 {
	barrier := NewConventionalBarrier(nodes)
	var wg sync.WaitGroup
	var totalDelay time.Duration
	var delayMu sync.Mutex

	start := time.Now()

	for i := 0; i < nodes; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				before := time.Now()
				barrier.Wait()
				delay := time.Since(before)

				delayMu.Lock()
				totalDelay += delay
				delayMu.Unlock()
			}
		}()
	}

	wg.Wait()
	_ = time.Since(start)

	return float64(totalDelay.Microseconds()) / float64(iterations*nodes) / 1000
}

func measureIntelligent(nodes int) float64 {
	var totalDelay time.Duration
	var delayMu sync.Mutex
	var wg sync.WaitGroup

	barrier := NewIntelligentBarrier(nodes)

	start := time.Now()

	for i := 0; i < nodes; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				before := time.Now()
				barrier.Wait()
				delay := time.Since(before)

				delayMu.Lock()
				totalDelay += delay
				delayMu.Unlock()
			}
		}()
	}

	wg.Wait()
	_ = time.Since(start)

	return float64(totalDelay.Microseconds()) / float64(iterations*nodes) / 1000
}

func main() {
	clusterSizes := []int{3, 5, 7, 9, 11}

	fmt.Println("Cluster Size,Conventional (ms),Intelligent (ms)")

	for _, nodes := range clusterSizes {
		conventional := measureConventional(nodes)
		intelligent := measureIntelligent(nodes)

		fmt.Printf(
			"%d,%.6f,%.6f\n",
			nodes,
			conventional,
			intelligent,
		)
	}
}
