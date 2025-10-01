package topk_test

import (
	"testing"
	heap "topk/lib"

	"github.com/stretchr/testify/require"
)

func TestEmptyHeap(t *testing.T) {
	h := heap.NewHeap(func(a, b int) int { return a - b })
	require.EqualValues(t, true, h.IsEmpty())
	require.EqualValues(t, 0, h.Size())
	require.Panics(t, func() { h.Top() })
	require.Panics(t, func() { h.Pop() })
}

func TestHeapPushPop(t *testing.T) {
	h := heap.NewHeap(func(a, b int) int { return a - b })
	h.Push(5)

	require.EqualValues(t, false, h.IsEmpty())
	require.EqualValues(t, 1, h.Size())
	require.EqualValues(t, 5, h.Top())

	require.EqualValues(t, 5, h.Pop())
	require.EqualValues(t, true, h.IsEmpty())
	require.EqualValues(t, 0, h.Size())
	require.Panics(t, func() { h.Top() })
	require.Panics(t, func() { h.Pop() })
}

func TestManyelements(t *testing.T) {
	h := heap.NewHeap(func(a, b int) int { return a - b })
	nums := []int{1, 2, 3, 4, 5, 6, 7, 8}
	for _, n := range nums {
		h.Push(n)
		require.EqualValues(t, n, h.Top())
		require.EqualValues(t, len(nums[:n]), h.Size())
	}

	for i := len(nums) - 1; i >= 0; i-- {
		require.EqualValues(t, nums[i], h.Top())
		require.EqualValues(t, nums[i], h.Pop())
		require.EqualValues(t, i, h.Size())
	}
	require.EqualValues(t, true, h.IsEmpty())
	require.EqualValues(t, 0, h.Size())
	require.Panics(t, func() { h.Top() })
	require.Panics(t, func() { h.Pop() })
}

type Persona struct {
	Name string
	Age  int
}

func TestHeapWithStructs(t *testing.T) {
	h := heap.NewHeap(func(a, b Persona) int { return a.Age - b.Age })
	personas := []Persona{
		{"Alice", 30},
		{"Bob", 25},
		{"Charlie", 35},
		{"David", 20},
	}

	for _, p := range personas {
		h.Push(p)
	}
	require.EqualValues(t, 4, h.Size())
	require.EqualValues(t, Persona{"Charlie", 35}, h.Top())
	require.EqualValues(t, Persona{"Charlie", 35}, h.Pop())
	require.EqualValues(t, 3, h.Size())
	require.EqualValues(t, Persona{"Alice", 30}, h.Top())
}
