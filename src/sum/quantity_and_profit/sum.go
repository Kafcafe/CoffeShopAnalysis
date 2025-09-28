package quantityandprofit

type Item struct {
	TotalQuantity int
	TotalProfit   float64
}

type ItemID int
type YearMonth string
type Record = string

type YearMonthSum struct {
	yearMonth YearMonth
	items     map[ItemID]Item
}

func New(yearMonth YearMonth) YearMonthSum {
	return YearMonthSum{
		yearMonth: yearMonth,
		items:     make(map[ItemID]Item),
	}
}

func sumItems(item1, item2 Item) Item {
	return Item{
		TotalQuantity: item1.TotalQuantity + item2.TotalQuantity,
		TotalProfit:   item1.TotalProfit + item2.TotalProfit,
	}
}

func (s YearMonthSum) addRecord(record Record) {
	parsedRecord, err := parseRecord(record)
	if err != nil {
		return
	}
	item := Item{
		TotalQuantity: parsedRecord.Quantity,
		TotalProfit:   parsedRecord.Profit,
	}
	existingItem, exists := s.items[parsedRecord.ItemID]
	if exists {
		item = sumItems(existingItem, item)
	}
	s.items[parsedRecord.ItemID] = item
}

func (s YearMonthSum) AddRecords(records []Record) {
	for _, record := range records {
		s.addRecord(record)
	}
}

func (s YearMonthSum) Get(itemID ItemID) Item {
	return s.items[itemID]
}
