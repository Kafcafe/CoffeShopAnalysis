package topk_test

import (
	"testing"
	heap "topk/lib"

	"github.com/stretchr/testify/require"
)

func TestGenerateTopKFromData(t *testing.T) {
	source := []int{5, 1, 3, 6}
	topper := heap.NewToper(1, func(a, b int) int { return b - a })

	for _, n := range source {
		topper.Add(n)
	}
	topK := topper.GetTopK()
	require.EqualValues(t, []int{6}, topK)
}

func TestGenerateTopKFromData2(t *testing.T) {
	source := []int{5, 1, 3, 6}
	topper := heap.NewToper(2, func(a, b int) int { return b - a })

	for _, n := range source {
		topper.Add(n)
	}
	topK := topper.GetTopK()
	require.EqualValues(t, []int{6, 5}, topK)
}

func TestGenerateTopKFromData3(t *testing.T) {
	source := []int{5, 1, 3, 6, 7, 9}
	topper := heap.NewToper(3, func(a, b int) int { return b - a })

	for _, n := range source {
		topper.Add(n)
	}
	topK := topper.GetTopK()
	require.EqualValues(t, []int{9, 7, 6}, topK)
}

func TestGenerateTopKWithDuplicatesAndNegatives(t *testing.T) {
	source := []int{5, -2, 9, 7, 9, 3, -2, 6, 7}
	topper := heap.NewToper(4, func(a, b int) int { return b - a })

	for _, n := range source {
		topper.Add(n)
	}
	topK := topper.GetTopK()
	require.EqualValues(t, []int{9, 9, 7, 7}, topK)
}
