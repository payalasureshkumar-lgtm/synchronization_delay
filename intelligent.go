package main

import (
	"fmt"
	"sync"
	"time"
)

const iterations = 1000

type NodeArrival struct {
	NodeID  int
	Load    float64
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

		for i := 0; i < b.total; i++ {
			batch = append(batch, <-b.arrivals)
		}

		for i := 0; i < len(batch); i++ {
			for j := i + 1; j < len(batch); j++ {
				if batch[j].Load < batch[i].Load {
					batch[i], batch[j] = batch[j], batch[i]
				}
			}
		}

		for _, node := range batch {
			close(node.Release)
		}
	}
}

func (b *ResourceAwareBarrier) Wait(nodeID int, load float64) {
	release := make(chan struct{})

	b.arrivals <- NodeArrival{
		NodeID:  nodeID,
		Load:    load,
		Release: release,
	}

	<-release
}

func main() {
	clusterSizes := []int{3, 5, 7, 9, 11}

	fmt.Println("Cluster Size,Intelligent Delay (ms)")

	for _, nodes := range clusterSizes {
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

					start := time.Now()
					barrier.Wait(nodeID, load)
					delay := time.Since(start)

					mu.Lock()
					totalDelay += delay
					mu.Unlock()
				}
			}(i)
		}

		wg.Wait()

		average := float64(totalDelay.Microseconds()) /
			float64(nodes*iterations) / 1000

		fmt.Printf("%d,%.6f\n", nodes, average)
	}
}
