package yearmonth

import (
	"testing"
)

func TestExtractYearMonth(t *testing.T) {
	record := "2ae6d188-76c2-4095-b861-ab97d3cd9312,6,3,9.5,28.5,2023-07-01 07:00:00"
	expected := YearMonth("2023-07")
	result := ExtractYearMonth(record)
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestGroup_AddAndGet(t *testing.T) {
	g := New()
	record1 := "2ae6d188-76c2-4095-b861-ab97d3cd9312,6,3,9.5,28.5,2023-07-01 07:00:00"
	record2 := "7d0a474d-62f4-442a-96b6-a5df2bda8832,5,3,9.0,27.0,2023-07-01 07:00:02"
	ym := YearMonth("2023-07")

	g.Add(record1)
	g.Add(record2)

	records := g.Get(ym)
	if len(records) != 2 {
		t.Errorf("Expected 2 records, got %d", len(records))
	}
}

func TestFromRecords(t *testing.T) {
	records := []Record{
		"2ae6d188-76c2-4095-b861-ab97d3cd9312,6,3,9.5,28.5,2023-07-01 07:00:00",
		"7d0a474d-62f4-442a-96b6-a5df2bda8832,5,3,9.0,27.0,2023-07-01 07:00:02",
		"85f86fef-fddb-4eef-9dc3-1444553e6108,7,3,9.0,27.0,2023-08-01 07:00:04",
	}
	g := New()
	g.AddRecords(records)

	if len(g) != 2 {
		t.Errorf("Expected 2 year-month groups, got %d", len(g))
	}

	if len(g[YearMonth("2023-07")]) != 2 {
		t.Errorf("Expected 2 records for 2023-07, got %d", len(g[YearMonth("2023-07")]))
	}

	if len(g[YearMonth("2023-08")]) != 1 {
		t.Errorf("Expected 1 record for 2023-08, got %d", len(g[YearMonth("2023-08")]))
	}
}
