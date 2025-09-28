package quantityandprofit

import "testing"

func TestAddRecords_SingleRecord(t *testing.T) {
	sum := New("2025-09")
	sum.AddRecords([]Record{"1,3,10.5"})

	item := sum.Get(1)
	if item.TotalQuantity != 3 || item.TotalProfit != 10.5 {
		t.Errorf("TotalQuantity = %d, expected 3. TotalProfit = %f, expected 10.5", item.TotalQuantity, item.TotalProfit)
	}
}

func TestAddRecords_MultipleSameItem(t *testing.T) {
	sum := New("2025-09")
	sum.AddRecords([]Record{
		"1,2,10.5",
		"1,3,20.0",
	})

	item := sum.Get(1)
	if item.TotalQuantity != 5 || item.TotalProfit != 30.5 {
		t.Errorf("TotalQuantity = %d, expected 5. TotalProfit = %f, expected 30.5", item.TotalQuantity, item.TotalProfit)
	}
}

func TestAddRecords_MultipleDifferentItems(t *testing.T) {
	sum := New("2025-09")
	sum.AddRecords([]Record{
		"1,2,10.5",
		"2,1,5.0",
	})

	item1 := sum.Get(1)
	if item1.TotalQuantity != 2 || item1.TotalProfit != 10.5 {
		t.Errorf("TotalQuantity = %d, expected 3. TotalProfit = %f, expected 10.5", item1.TotalQuantity, item1.TotalProfit)
	}

	item2 := sum.Get(2)
	if item2.TotalQuantity != 1 || item2.TotalProfit != 5.0 {
		t.Errorf("TotalQuantity = %d, expected 1. TotalProfit = %f, expected 5.0", item2.TotalQuantity, item2.TotalProfit)
	}
}

func TestAddRecords_InvalidRecordIgnored(t *testing.T) {
	sum := New("2025-09")
	sum.AddRecords([]Record{
		"x,2,10.5", // invalid
		"1,2,20.0", // valid
	})

	item := sum.Get(1)
	if item.TotalQuantity != 2 || item.TotalProfit != 20.0 {
		t.Errorf("TotalQuantity = %d, expected 2. TotalProfit = %f, expected 20.0", item.TotalQuantity, item.TotalProfit)
	}

	itemInvalid := sum.Get(99)
	if itemInvalid.TotalQuantity != 0 || itemInvalid.TotalProfit != 0 {
		t.Errorf("Expected empty item, got %+v", itemInvalid)
	}
}

func TestAddRecords_EmptyInput(t *testing.T) {
	sum := New("2025-09")
	sum.AddRecords([]Record{})

	item := sum.Get(1)
	if item.TotalQuantity != 0 || item.TotalProfit != 0 {
		t.Errorf("Expected empty item, got %+v", item)
	}
}
