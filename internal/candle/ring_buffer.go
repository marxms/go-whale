package candle

// RingBuffer is a generic fixed-capacity circular buffer.
// Push overwrites the oldest element when full. Push, PeekLatest, Len and Cap
// are O(1) and allocation-free; Slice/SliceDesc are O(n) and allocate an
// n-element slice. The snapshot flush hot path uses PeekLatest (via
// CandleEngine.PeekLatestCandle) to avoid the SliceDesc allocation.
type RingBuffer[T any] struct {
	data  []T
	head  int // next write position
	count int // current number of elements
	cap   int
}

// NewRingBuffer creates a RingBuffer with the given capacity.
// Panics if capacity <= 0.
func NewRingBuffer[T any](capacity int) *RingBuffer[T] {
	if capacity <= 0 {
		panic("RingBuffer capacity must be > 0")
	}
	return &RingBuffer[T]{
		data: make([]T, capacity),
		cap:  capacity,
	}
}

// Push appends an item, overwriting the oldest when full.
func (rb *RingBuffer[T]) Push(item T) {
	rb.data[rb.head] = item
	rb.head = (rb.head + 1) % rb.cap
	if rb.count < rb.cap {
		rb.count++
	}
}

// PeekLatest returns the most recently pushed item.
func (rb *RingBuffer[T]) PeekLatest() (T, bool) {
	if rb.count == 0 {
		var zero T
		return zero, false
	}
	idx := (rb.head - 1 + rb.cap) % rb.cap
	return rb.data[idx], true
}

// Slice returns elements oldest → newest.
func (rb *RingBuffer[T]) Slice() []T {
	if rb.count == 0 {
		return nil
	}
	out := make([]T, rb.count)
	start := (rb.head - rb.count + rb.cap) % rb.cap
	for i := range rb.count {
		out[i] = rb.data[(start+i)%rb.cap]
	}
	return out
}

// SliceDesc returns elements newest → oldest.
func (rb *RingBuffer[T]) SliceDesc() []T {
	if rb.count == 0 {
		return nil
	}
	out := make([]T, rb.count)
	for i := range rb.count {
		idx := (rb.head - 1 - i + rb.cap) % rb.cap
		out[i] = rb.data[idx]
	}
	return out
}

// At returns the element at logical index i, where 0 is the oldest.
func (rb *RingBuffer[T]) At(i int) (T, bool) {
	if i < 0 || i >= rb.count {
		var zero T
		return zero, false
	}
	start := (rb.head - rb.count + rb.cap) % rb.cap
	return rb.data[(start+i)%rb.cap], true
}

// SetAt replaces the element at logical index i, where 0 is the oldest.
func (rb *RingBuffer[T]) SetAt(i int, item T) bool {
	if i < 0 || i >= rb.count {
		return false
	}
	start := (rb.head - rb.count + rb.cap) % rb.cap
	rb.data[(start+i)%rb.cap] = item
	return true
}

// UpdateLatest mutates the newest element in place. Callers must
// synchronize access to the buffer.
func (rb *RingBuffer[T]) UpdateLatest(fn func(*T)) bool {
	if rb.count == 0 {
		return false
	}
	idx := (rb.head - 1 + rb.cap) % rb.cap
	fn(&rb.data[idx])
	return true
}

// SetLatest replaces the newest element. It is a no-op on an empty buffer.
func (rb *RingBuffer[T]) SetLatest(item T) bool {
	return rb.UpdateLatest(func(current *T) { *current = item })
}

// Oldest returns the oldest element still held.
func (rb *RingBuffer[T]) Oldest() (T, bool) {
	if rb.count == 0 {
		var zero T
		return zero, false
	}
	start := (rb.head - rb.count + rb.cap) % rb.cap
	return rb.data[start], true
}

// Reset drops every element and keeps the allocated capacity.
func (rb *RingBuffer[T]) Reset() {
	rb.head = 0
	rb.count = 0
}

// Len returns the current number of elements.
func (rb *RingBuffer[T]) Len() int { return rb.count }

// Cap returns the buffer capacity.
func (rb *RingBuffer[T]) Cap() int { return rb.cap }
