package datatypes

type Heap[T comparable] struct {
	amount   int
	data     []T
	Comparer func(T, T) int
}

const (
	InitialSize   = 16
	ScalingFactor = 2
	Error         = "Heap is empty"
)

func NewHeap[T comparable](comparer func(T, T) int) *Heap[T] {
	heap := new(Heap[T])
	heap.data = make([]T, InitialSize)
	heap.Comparer = comparer
	return heap
}

func (h *Heap[T]) IsEmpty() bool {
	return h.Size() == 0
}

func (h *Heap[T]) Size() int {
	return h.amount
}

func (h *Heap[T]) Top() T {
	if h.IsEmpty() {
		panic(Error)
	}
	return h.data[0]
}

func (h *Heap[T]) Pop() T {
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

func (h *Heap[T]) Push(value T) {
	h.redimensionIfNeeded()
	h.data[h.amount] = value
	h.upHeap(h.amount)
	h.amount++
}

func (h *Heap[T]) upHeap(currentPost int) {
	parentPos := mod((currentPost - 1) / 2)

	if parentPos == currentPost || h.Comparer(h.data[currentPost], h.data[parentPos]) <= 0 {
		return
	}
	h.swap(currentPost, parentPos)
	h.upHeap(parentPos)
}

func (h *Heap[T]) downHeap(currentPos int) {
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

func (h *Heap[T]) swapAndDownHeap(greater, currentPos int) {

	if h.Comparer(h.data[greater], h.data[currentPos]) > 0 {
		h.swap(currentPos, greater)
		h.downHeap(greater)
	}
}

func (h *Heap[T]) redimensionIfNeeded() {
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

func (h *Heap[T]) swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *Heap[T]) max(pos1, pos2 int) int {
	if h.Comparer(h.data[pos1], h.data[pos2]) > 0 {
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
