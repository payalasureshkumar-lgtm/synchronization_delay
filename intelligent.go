package main

import (
	"fmt"
	"sync"
	"time"
)

const (
	iterations = 1000
)

type NodeArrival struct {
	NodeID  int
	Load    float64
	Arrival time.Time
	Release chan struct{}
}

type ResourceAwareBarrier struct {
	total    int
	arrivals chan NodeArrival
}

func NewResourceAwareBarrier(total int) *ResourceAwareBarrier {
	b := &ResourceAwareBarrier{
		total:    total,
		arrivals: make(chan NodeArrival, total),
	}

	go b.coordinate()
	return b
}

func (b *ResourceAwareBarrier) coordinate() {
	for {
		batch := make([]NodeArrival, 0, b.total)
		totalLoad := 0.0

		for i := 0; i < b.total; i++ {
			arrival := <-b.arrivals
			batch = append(batch, arrival)
			totalLoad += arrival.Load
		}

		averageLoad := totalLoad / float64(b.total)

		for _, node := range batch {
			if node.Load > averageLoad {
				time.Sleep(time.Microsecond)
			}
		}

		for _, node := range batch {
			close(node.Release)
		}
	}
}

func (b *ResourceAwareBarrier) Wait(nodeID int, load float64) time.Duration {
	release := make(chan struct{})

	b.arrivals <- NodeArrival{
		NodeID:  nodeID,
		Load:    load,
		Arrival: time.Now(),
		Release: release,
	}

	start := time.Now()
	<-release

	return time.Since(start)
}

func measureIntelligent(nodes int) float64 {
	barrier := NewResourceAwareBarrier(nodes)

	var wg sync.WaitGroup
	var totalDelay time.Duration
	var mu sync.Mutex

	for i := 0; i < nodes; i++ {
		wg.Add(1)

		go func(nodeID int) {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				load := float64((nodeID*17+j*13)%100) / 100.0

				delay := barrier.Wait(nodeID, load)

				mu.Lock()
				totalDelay += delay
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	return float64(totalDelay.Microseconds()) /
		float64(iterations*nodes) / 1000
}

func main() {
	clusterSizes := []int{3, 5, 7, 9, 11}

	fmt.Println("Cluster Size,Intelligent Synchronization Delay (ms)")

	for _, nodes := range clusterSizes {
		delay := measureIntelligent(nodes)
		fmt.Printf("%d,%.6f\n", nodes, delay)
	}
}
