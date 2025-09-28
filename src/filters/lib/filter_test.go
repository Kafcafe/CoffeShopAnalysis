package filter_test

import (
	lib "filters/lib"
	"testing"

	"github.com/stretchr/testify/require"
)

var batchExample = []string{
	"2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2023-07-01 07:00:00",
	"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2023-07-01 07:00:02",
	"85f86fef-fddb-4eef-9dc3-1444553e6108,1,5,,,27.0,0.0,27.0,2023-07-01 07:00:04",
	"4c41d179-f809-4d5a-a5d7-acb25ae1fe98,5,2,,,45.5,0.0,45.5,2023-07-01 07:00:21",
	"51e44c8e-4812-4a15-a9f9-9a46b62424d6,8,5,,,27.0,0.0,27.0,2023-07-01 07:00:33",
	"d449cf8f-e6d5-4b09-a02e-693c7889dee8,8,3,,,45.0,0.0,45.0,2023-07-01 07:00:44",
	"6b00c575-ec6e-4070-82d2-26d66b017b8b,3,1,,,77.0,0.0,77.0,2023-07-01 07:00:57",
	"54fa4304-5131-4382-a8dc-f30cb18155b7,5,5,,,47.0,0.0,47.0,2023-07-01 07:01:01",
	"bc9a368b-50d6-4f16-9505-edd8019c95ba,5,5,,,27.0,0.0,27.0,2023-07-01 07:01:20",
	"fe97c4a3-bbef-493d-ae59-d4574132a8ae,2,2,,,43.0,0.0,43.0,2023-07-01 07:01:22",
}

func TestNewFilter(t *testing.T) {
	filter := lib.NewFilter()
	require.NotNil(t, filter, "Expected NewFilter to return a non-nil Filter instance")
}

func TestFilterByDatetimeHourEmpty(t *testing.T) {
	filter := lib.NewFilter()
	result := filter.FilterByDatetimeHour([]string{})
	require.Equal(t, []string{}, result, "Expected FilterByDatetimeHour to return an empty slice")
}

func TestFilterByYearMonthEmpty(t *testing.T) {
	filter := lib.NewFilter()
	result := filter.FilterByYearMonth([]string{})
	require.Equal(t, []string{}, result, "Expected FilterByYearMonth to return an empty slice")
}

func TestFilterByAmountEmpty(t *testing.T) {
	filter := lib.NewFilter()
	result := filter.FilterByAmount([]string{}, 10)
	require.Equal(t, []string{}, result, "Expected FilterByAmount to return an empty slice")
}

func TestFilterByAmountBatch(t *testing.T) {
	filter := lib.NewFilter()
	result := filter.FilterByAmount(batchExample, 40)
	expectedResult := []string{
		"4c41d179-f809-4d5a-a5d7-acb25ae1fe98,5,2,,,45.5,0.0,45.5,2023-07-01 07:00:21",
		"d449cf8f-e6d5-4b09-a02e-693c7889dee8,8,3,,,45.0,0.0,45.0,2023-07-01 07:00:44",
		"6b00c575-ec6e-4070-82d2-26d66b017b8b,3,1,,,77.0,0.0,77.0,2023-07-01 07:00:57",
		"54fa4304-5131-4382-a8dc-f30cb18155b7,5,5,,,47.0,0.0,47.0,2023-07-01 07:01:01",
		"fe97c4a3-bbef-493d-ae59-d4574132a8ae,2,2,,,43.0,0.0,43.0,2023-07-01 07:01:22",
	}
	require.Equal(t, expectedResult, result, "Expected FilterByAmount to return the correct filtered slice")
}

func TestFilterBatchByAmount2(t *testing.T) {
	filter := lib.NewFilter()
	result := filter.FilterByAmount(batchExample, 70)
	expectedResult := []string{
		"6b00c575-ec6e-4070-82d2-26d66b017b8b,3,1,,,77.0,0.0,77.0,2023-07-01 07:00:57",
	}
	require.Equal(t, expectedResult, result, "Expected FilterByAmount to return the correct filtered slice")
}

func TestFilterBatchByAmountAllFiltered(t *testing.T) {
	filter := lib.NewFilter()
	result := filter.FilterByAmount(batchExample, 80)
	expectedResult := []string{}
	require.Equal(t, expectedResult, result, "Expected FilterByAmount to return the correct filtered slice")
}
