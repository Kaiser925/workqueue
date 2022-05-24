package workqueue

import (
	"k8s.io/utils/clock"
	"sync"
	"time"
)

// Interface is the interface for a work queue.
type Interface[T comparable] interface {
	// Add adds a new item to the work queue.
	Add(item T)
	// Len returns the number of items in the work queue.
	Len() int
	// Get returns the next item in the work queue and marks it as done.
	Get() (T, shutdown bool)
	// Done marks the item as done.
	Done(item T)
	// ShutDown shuts down the work queue.
	ShutDown()
	// ShutDownWithDrain will shut down the work queue and drain the queue.
	ShutDownWithDrain()
	// ShuttingDown returns true if the work queue is shutting down.
	ShuttingDown() bool
}

// FIFO is a basic FIFO work queue.
type FIFO[T comparable] struct {
	// queue defines the order in which we will work on items. Every
	// element of queue should be in the dirty set and not in the
	// processing set.
	queue []*T

	// dirty defines all of the items that need to be processed.
	dirty set[T]

	// Things that are currently being processed are in the processing set.
	// These things may be simultaneously in the dirty set. When we finish
	// processing something and remove it from this set, we'll check if
	// it's in the dirty set, and if so, add it to the queue.
	processing set[T]

	cond *sync.Cond

	shuttingDown bool
	drain        bool

	metrics queueMetrics[T]

	unfinishedWorkUpdatePeriod time.Duration
	clock                      clock.WithTicker
}

type empty struct{}

type set[T comparable] map[T]empty

func (s set[T]) has(item T) bool {
	_, exists := s[item]
	return exists
}

func (s set[T]) insert(item T) {
	s[item] = empty{}
}

func (s set[T]) delete(item T) {
	delete(s, item)
}

func (s set[T]) len() int {
	return len(s)
}

// Add marks item as needing processing.
func (q *FIFO[T]) Add(item T) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return
	}
	if q.dirty.has(item) {
		return
	}

	q.metrics.add(item)

	q.dirty.insert(item)
	if q.processing.has(item) {
		return
	}

	q.queue = append(q.queue, &item)
	q.cond.Signal()
}

// Len returns the current queue length, for informational purposes only. You
// shouldn't e.g. gate a call to Add() or Get() on Len() being a particular
// value, that can't be synchronized properly.
func (q *FIFO[T]) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return len(q.queue)
}

// Get blocks until it can return an item to be processed. If shutdown = true,
// the caller should end their goroutine. You must call Done with item when you
// have finished processing it.
func (q *FIFO[T]) Get() (item T, shutdown bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for len(q.queue) == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if len(q.queue) == 0 {
		// We must be shutting down.
		var noop T
		return noop, true
	}

	item = *q.queue[0]
	// The underlying array still exists and reference this object, so the object will not be garbage collected.
	q.queue[0] = nil
	q.queue = q.queue[1:]

	q.metrics.get(item)

	q.processing.insert(item)
	q.dirty.delete(item)

	return item, false
}

// Done marks item as done processing, and if it has been marked as dirty again
// while it was being processed, it will be re-added to the queue for
// re-processing.
func (q *FIFO[T]) Done(item T) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.metrics.done(item)

	q.processing.delete(item)
	if q.dirty.has(item) {
		q.queue = append(q.queue, &item)
		q.cond.Signal()
	} else if q.processing.len() == 0 {
		q.cond.Signal()
	}
}

// ShutDown will cause q to ignore all new items added to it and
// immediately instruct the worker goroutines to exit.
func (q *FIFO[T]) ShutDown() {
	q.setDrain(false)
	q.shutdown()
}

func (q *FIFO[T]) setDrain(shouldDrain bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.drain = shouldDrain
}

func (q *FIFO[T]) shouldDrain() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.drain
}

func (q *FIFO[T]) shutdown() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.shuttingDown = true
	q.cond.Broadcast()
}

func (q *FIFO[T]) updateUnfinishedWorkLoop() {
	t := q.clock.NewTicker(q.unfinishedWorkUpdatePeriod)
	defer t.Stop()
	for range t.C() {
		if !func() bool {
			q.cond.L.Lock()
			defer q.cond.L.Unlock()
			if !q.shuttingDown {
				q.metrics.updateUnfinishedWork()
				return true
			}
			return false

		}() {
			return
		}
	}
}

func newQueue[T comparable](c clock.WithTicker, metrics queueMetrics[T], updatePeriod time.Duration) *FIFO[T] {
	t := &FIFO[T]{
		clock:                      c,
		dirty:                      set[T]{},
		processing:                 set[T]{},
		cond:                       sync.NewCond(&sync.Mutex{}),
		metrics:                    metrics,
		unfinishedWorkUpdatePeriod: updatePeriod,
	}

	// Don't start the goroutine for a type of noMetrics so we don't consume
	// resources unnecessarily
	if _, ok := metrics.(noMetrics[T]); !ok {
		go t.updateUnfinishedWorkLoop()
	}

	return t
}
