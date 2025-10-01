package topk

type heap[T comparable] struct {
	amount   int
	data     []T
	comparer func(T, T) int
}

const (
	InitialSize   = 16
	ScalingFactor = 2
	Error         = "Heap is empty"
)

func NewHeap[T comparable](comparer func(T, T) int) *heap[T] {
	heap := new(heap[T])
	heap.data = make([]T, InitialSize)
	heap.comparer = comparer
	return heap
}

func (h *heap[T]) IsEmpty() bool {
	return h.Size() == 0
}

func (h *heap[T]) Size() int {
	return h.amount
}

func (h *heap[T]) Top() T {
	if h.IsEmpty() {
		panic(Error)
	}
	return h.data[0]
}

func (h *heap[T]) Pop() T {
	if h.IsEmpty() {
		panic(Error)
	}

	returnData := h.data[0]
	h.amount--
	h.redimensionIfNeeded()
	h.swap(0, h.amount)
	h.downHeap(0)
	return returnData
}

func (h *heap[T]) Push(value T) {
	h.redimensionIfNeeded()
	h.data[h.amount] = value
	h.upHeap(h.amount)
	h.amount++
}

func (h *heap[T]) upHeap(currentPost int) {
	parentPos := mod((currentPost - 1) / 2)

	if parentPos == currentPost || h.comparer(h.data[currentPost], h.data[parentPos]) <= 0 {
		return
	}
	h.swap(currentPost, parentPos)
	h.upHeap(parentPos)
}

func (h *heap[T]) downHeap(currentPos int) {
	leftChildPos := currentPos*2 + 1
	rightChildPos := currentPos*2 + 2

	if rightChildPos < h.amount {
		greater := h.max(leftChildPos, rightChildPos)
		h.swapAndDownHeap(greater, currentPos)
	}

	if leftChildPos < h.amount {
		greater := leftChildPos
		h.swapAndDownHeap(greater, currentPos)
	}
}

func (h *heap[T]) swapAndDownHeap(greater, currentPos int) {

	if h.comparer(h.data[greater], h.data[currentPos]) > 0 {
		h.swap(currentPos, greater)
		h.downHeap(greater)
	}
}

func (h *heap[T]) redimensionIfNeeded() {
	if h.amount == cap(h.data) || len(h.data) == h.amount {
		newData := make([]T, cap(h.data)*ScalingFactor)
		copy(newData, h.data)
		h.data = newData
		return
	}

	if h.amount > InitialSize && h.amount*4 <= cap(h.data) {
		newData := make([]T, cap(h.data)/ScalingFactor)
		copy(newData, h.data)
		h.data = newData
		return
	}
}

func (h *heap[T]) swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *heap[T]) max(pos1, pos2 int) int {
	if h.comparer(h.data[pos1], h.data[pos2]) > 0 {
		return pos1
	}
	return pos2
}

func mod(a int) int {
	if a < 0 {
		return -a
	}
	return a
}
