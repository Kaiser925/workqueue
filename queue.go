package workqueue

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
