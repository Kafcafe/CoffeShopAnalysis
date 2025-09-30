package group

import "strings"

type YearMonth string
type Record = string

type Group map[YearMonth][]Record

func New() Group {
	return make(Group)
}

func (g Group) AddRecords(records []Record) {
	for _, record := range records {
		g.Add(record)
	}
}

func (g Group) Get(yearMonth YearMonth) []Record {
	return g[yearMonth]
}

func (g Group) Add(record Record) {
	ym := ExtractYearMonth(record)
	g[ym] = append(g[ym], record)
}

func ExtractYearMonth(record Record) YearMonth {
	// Assuming the date is in the format "YYYY-MM-DD HH:MM:SS"
	// Assumming item_id,quantity,subtotal,date
	// Example: "6,3,28.5,2023-07-01 07:00:00"
	fields := strings.Split(record, ",")
	dateField := fields[len(fields)-1]
	dateField = strings.TrimSpace(dateField)
	// Extract "YYYY-MM"
	yearMonth := dateField[:7]
	return YearMonth(yearMonth)
}
