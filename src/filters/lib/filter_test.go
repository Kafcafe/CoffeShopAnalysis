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

var BatchExample2WithHours = []string{
	"2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2023-07-01 06:15:00",
	"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2023-07-01 07:00:02",
	"85f86fef-fddb-4eef-9dc3-1444553e6108,1,5,,,27.0,0.0,27.0,2023-07-01 08:30:04",
	"4c41d179-f809-4d5a-a5d7-acb25ae1fe98,5,2,,,45.5,0.0,45.5,2023-07-01 09:45:21",
	"51e44c8e-4812-4a15-a9f9-9a46b62424d6,8,5,,,27.0,0.0,27.0,2023-07-01 10:00:33",
	"d449cf8f-e6d5-4b09-a02e-693c7889dee8,8,3,,,45.0,0.0,45.0,2023-07-01 11:20:44",
	"6b00c575-ec6e-4070-82d2-26d66b017b8b,3,1,,,77.0,0.0,77.0,2023-07-01 12:00:57",
	"54fa4304-5131-4382-a8dc-f30cb18155b7,5,5,,,47.0,0.0,47.0,2023-07-01 13:01:01",
	"bc9a368b-50d6-4f16-9505-edd8019c95ba,5,5,,,27.0,0.0,27.0,2023-07-01 14:01:20",
	"fe97c4a3-bbef-493d-ae59-d4574132a8ae,2,2,,,43.0,0.0,43.0,2023-07-01 15:01:22",
	"85508712-16bc-474f-af7c-23966680e76e,8,1,,,42.0,0.0,42.0,2023-07-01 16:01:23",
	"d921113a-a18f-496e-9283-d62182f9322d,5,3,,,54.5,0.0,54.5,2023-07-01 17:01:34",
	"725e0e23-02e7-43a9-9162-3089ed93ef9c,6,1,,,54.0,0.0,54.0,2023-07-01 18:01:37",
	"ed939e86-8545-4632-991a-1e523d2c36a8,7,2,,,69.5,0.0,69.5,2023-07-01 19:01:42",
	"6108f270-73ef-4fc1-8e6d-107f3d8082ef,4,1,,,43.0,0.0,43.0,2023-07-01 20:01:43",
	"eb89be5f-db4f-4e0d-9196-8bebaac57f33,6,1,,,36.0,0.0,36.0,2023-07-01 21:01:52",
	"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 22:01:54",
	"825e124c-dcc0-4e07-82fb-1b4634a49808,2,2,,,19.0,0.0,19.0,2023-07-01 23:02:15",
	"928498fd-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2023-07-01 00:02:21",
}

func TestNewFilter(t *testing.T) {
	filter := lib.NewFilter()
	require.NotNil(t, filter, "Expected NewFilter to return a non-nil Filter instance")
}

func TestFilterByDatetimeHourEmpty(t *testing.T) {
	filter := lib.NewFilter()
	result := filter.FilterByDatetimeHour([]string{}, 2023, 2023, 6, 23)
	require.Equal(t, []string{}, result, "Expected FilterByDatetimeHour to return an empty slice")
}

func TestFilterByYearMonthEmpty(t *testing.T) {
	filter := lib.NewFilter()
	result := filter.FilterByYear([]string{}, 2023, 2023)
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

func TestFilterByYearMonthBatch(t *testing.T) {
	filter := lib.NewFilter()
	result := filter.FilterByYear(batchExample, 2023, 2023)
	require.Equal(t, batchExample, result, "Expected FilterByYear to return the full batch")
}

func TestFilterByYearMonthBatchNone(t *testing.T) {
	filter := lib.NewFilter()
	result := filter.FilterByYear(batchExample, 2022, 2022)
	expectedResult := []string{}
	require.Equal(t, expectedResult, result, "Expected FilterByYear to return an empty slice")
}

func TestFilterByDatetimeHourBatch(t *testing.T) {
	filter := lib.NewFilter()
	result := filter.FilterByDatetimeHour(BatchExample2WithHours, 2023, 2023, 6, 23)
	expected := []string{
		"2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2023-07-01 06:15:00",
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2023-07-01 07:00:02",
		"85f86fef-fddb-4eef-9dc3-1444553e6108,1,5,,,27.0,0.0,27.0,2023-07-01 08:30:04",
		"4c41d179-f809-4d5a-a5d7-acb25ae1fe98,5,2,,,45.5,0.0,45.5,2023-07-01 09:45:21",
		"51e44c8e-4812-4a15-a9f9-9a46b62424d6,8,5,,,27.0,0.0,27.0,2023-07-01 10:00:33",
		"d449cf8f-e6d5-4b09-a02e-693c7889dee8,8,3,,,45.0,0.0,45.0,2023-07-01 11:20:44",
		"6b00c575-ec6e-4070-82d2-26d66b017b8b,3,1,,,77.0,0.0,77.0,2023-07-01 12:00:57",
		"54fa4304-5131-4382-a8dc-f30cb18155b7,5,5,,,47.0,0.0,47.0,2023-07-01 13:01:01",
		"bc9a368b-50d6-4f16-9505-edd8019c95ba,5,5,,,27.0,0.0,27.0,2023-07-01 14:01:20",
		"fe97c4a3-bbef-493d-ae59-d4574132a8ae,2,2,,,43.0,0.0,43.0,2023-07-01 15:01:22",
		"85508712-16bc-474f-af7c-23966680e76e,8,1,,,42.0,0.0,42.0,2023-07-01 16:01:23",
		"d921113a-a18f-496e-9283-d62182f9322d,5,3,,,54.5,0.0,54.5,2023-07-01 17:01:34",
		"725e0e23-02e7-43a9-9162-3089ed93ef9c,6,1,,,54.0,0.0,54.0,2023-07-01 18:01:37",
		"ed939e86-8545-4632-991a-1e523d2c36a8,7,2,,,69.5,0.0,69.5,2023-07-01 19:01:42",
		"6108f270-73ef-4fc1-8e6d-107f3d8082ef,4,1,,,43.0,0.0,43.0,2023-07-01 20:01:43",
		"eb89be5f-db4f-4e0d-9196-8bebaac57f33,6,1,,,36.0,0.0,36.0,2023-07-01 21:01:52",
		"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 22:01:54",
	}
	require.Equal(t, expected, result, "Expected FilterByDatetimeHour to return the full batch")
}

func TestFilterByDatetimeHourBatchNone(t *testing.T) {
	filter := lib.NewFilter()
	result := filter.FilterByDatetimeHour(BatchExample2WithHours, 2022, 2022, 6, 23)
	expectedResult := []string{}
	require.Equal(t, expectedResult, result, "Expected FilterByDatetimeHour to return an empty slice")
}
