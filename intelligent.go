package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

const iterations = 1000

type NodeState struct {
	NodeID      int
	ResourceLoad float64
	ArrivalTime time.Time
	Release      chan struct{}
}

type ResourceAwareBarrier struct {
	total    int
	arrivals chan NodeState
}

func NewResourceAwareBarrier(total int) *ResourceAwareBarrier {
	b := &ResourceAwareBarrier{
		total:    total,
		arrivals: make(chan NodeState, total),
	}

	go b.coordinate()

	return b
}

func (b *ResourceAwareBarrier) coordinate() {
	for {
		batch := make([]NodeState, 0, b.total)

		for i := 0; i < b.total; i++ {
			batch = append(batch, <-b.arrivals)
		}

		sort.Slice(batch, func(i, j int) bool {
			return batch[i].ResourceLoad < batch[j].ResourceLoad
		})

		for _, node := range batch {
			close(node.Release)
		}
	}
}

func (b *ResourceAwareBarrier) Wait(nodeID int, resourceLoad float64) {
	release := make(chan struct{})

	b.arrivals <- NodeState{
		NodeID:       nodeID,
		ResourceLoad: resourceLoad,
		ArrivalTime:  time.Now(),
		Release:      release,
	}

	<-release
}

func runCluster(nodes int) float64 {
	barrier := NewResourceAwareBarrier(nodes)

	var wg sync.WaitGroup
	var totalDelay time.Duration
	var mu sync.Mutex

	for i := 0; i < nodes; i++ {
		wg.Add(1)

		go func(nodeID int) {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				resourceLoad := float64((nodeID*17+j*13)%100) / 100.0

				start := time.Now()
				barrier.Wait(nodeID, resourceLoad)
				delay := time.Since(start)

				mu.Lock()
				totalDelay += delay
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	return float64(totalDelay.Nanoseconds()) /
		float64(nodes*iterations) / 1e6
}

func main() {
	clusterSizes := []int{3, 5, 7, 9, 11}

	fmt.Println("Cluster Size,Intelligent Delay (ms)")

	for _, nodes := range clusterSizes {
		delay := runCluster(nodes)
		fmt.Printf("%d,%.6f\n", nodes, delay)
	}
}
