package topk

type Toper[T comparable] struct {
	heap *heap[T]
	k    int
}

func NewToper[T comparable](k int, comparer func(T, T) int) *Toper[T] {
	if k <= 0 {
		return nil
	}

	return &Toper[T]{
		heap: NewHeap(comparer),
		k:    k,
	}
}

func NewToperWithSource[T comparable](k int, source []T, comparer func(T, T) int) *Toper[T] {
	toper := NewToper(k, comparer)

	for i := 0; i < k && i < len(source); i++ {
		toper.heap.Push(source[i])
	}

	return toper
}

func (t *Toper[T]) Add(value T) {
	if t.heap.Size() < t.k {
		t.heap.Push(value)
		return
	}

	if t.heap.comparer(value, t.heap.Top()) < 0 {
		t.heap.Pop()
		t.heap.Push(value)
	}
}

func (t *Toper[T]) GetTopK() []T {
	result := make([]T, t.k)
	for i := len(result) - 1; i >= 0; i-- {
		result[i] = t.heap.Pop()
	}
	return result
}
