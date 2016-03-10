package pq

import (
	"container/heap"
	tf "github.com/glycerine/tmframe"
	"time"
)

// Pqe (priority queue entry) is something we manage in a priority queue.
type Pqe struct {
	Val     *tf.Frame
	OrderBy time.Time // The priority of the item in the queue, earlier first.

	// The Idx is needed by Update and is maintained by the heap.Interface methods.
	Idx int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Pqes.
type PriorityQueue struct {
	Seq []*Pqe
}

func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{
		Seq: make([]*Pqe, 0),
	}
}

func (pq *PriorityQueue) First() *Pqe {
	return pq.Seq[0]
}

func (pq *PriorityQueue) Len() int { return len(pq.Seq) }

func (pq *PriorityQueue) Less(i, j int) bool {
	return pq.Seq[i].OrderBy.Before(pq.Seq[j].OrderBy)
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.Seq[i], pq.Seq[j] = pq.Seq[j], pq.Seq[i]
	pq.Seq[i].Idx = i
	pq.Seq[j].Idx = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(pq.Seq)
	item := x.(*Pqe)
	item.Idx = n
	pq.Seq = append(pq.Seq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := pq.Seq
	n := len(old)
	item := old[n-1]
	item.Idx = -1 // for safety
	pq.Seq = old[0 : n-1]
	return item
}

// Update modifies the priority and value of an Pqe in the queue.
// The pqe must already be in the queue at pqe.Idx location, as
// when it was returned by PriorityQueue.Add().
func (pq *PriorityQueue) Update(pqe *Pqe, value *tf.Frame) {
	pqe.Val = value
	pqe.OrderBy = time.Unix(0, value.Tm())
	heap.Fix(pq, pqe.Idx)
}

func (pq *PriorityQueue) Add(frame *tf.Frame) (*Pqe, error) {
	pqe := &Pqe{
		Val:     frame,
		OrderBy: time.Unix(0, frame.Tm()),
		Idx:     len(pq.Seq),
	}
	pq.Seq = append(pq.Seq, pqe)
	heap.Fix(pq, pqe.Idx)
	return pqe, nil
}

func (pq *PriorityQueue) Reinit() {
	heap.Init(pq)
}
