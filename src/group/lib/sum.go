package group

// func New(yearMonth YearMonth) YearMonthSum {
// 	return YearMonthSum{
// 		yearMonth: yearMonth,
// 		items:     make(map[ItemID]Item),
// 	}
// }

// func (s YearMonthSum) addRecord(record Record) {
// 	parsedRecord, err := parseRecord(record)
// 	if err != nil {
// 		return
// 	}
// 	item := Item{
// 		TotalQuantity: parsedRecord.Quantity,
// 		TotalProfit:   parsedRecord.Profit,
// 	}
// 	existingItem, exists := s.items[parsedRecord.ItemID]
// 	if exists {
// 		item = sumItems(existingItem, item)
// 	}
// 	s.items[parsedRecord.ItemID] = item
// }

// func (s YearMonthSum) AddRecords(records []Record) {
// 	for _, record := range records {
// 		s.addRecord(record)
// 	}
// }

// func (s YearMonthSum) Get(itemID ItemID) Item {
// 	return s.items[itemID]
// }
