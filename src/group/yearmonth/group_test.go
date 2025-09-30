package yearmonth

import (
	"testing"
)

func TestExtractYearMonth(t *testing.T) {
	record := "6,3,28.5,2023-07-01 07:00:00"
	expected := YearMonth("2023-07")
	result := ExtractYearMonth(record)
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestGroup_AddAndGet(t *testing.T) {
	g := New()
	record1 := "6,3,28.5,2023-07-01 07:00:00"
	record2 := "5,3,27.0,2023-07-01 07:00:02"
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
		"6,3,28.5,2023-07-01 07:00:00",
		"5,3,27.0,2023-07-01 07:00:02",
		"7,3,27.0,2023-08-01 07:00:04",
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
