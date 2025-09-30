package group

import (
	"runtime"
	"strings"
)

type YearMonth string
type Record = string

type YearMonthGroup map[YearMonth][]Record

func NewYearMonthGroup() YearMonthGroup {
	return make(YearMonthGroup)
}

func (g *YearMonthGroup) AddRecords(records []Record) {
	for _, record := range records {
		g.Add(record)
	}
}

func (g *YearMonthGroup) Get(yearMonth YearMonth) []Record {
	return (*g)[yearMonth]
}

func (g *YearMonthGroup) GetAll() map[YearMonth][]Record {
	return (*g)
}

func (g *YearMonthGroup) AddBatch(records []Record) {
	for _, record := range records {
		g.Add(record)
	}
}

func (g *YearMonthGroup) Add(record Record) {
	ym := ExtractYearMonth(record)
	(*g)[ym] = append((*g)[ym], record)
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

func (g YearMonthGroup) ToMapString() map[string][]string {
	out := make(map[string][]string, len(g))
	for ym, records := range g {
		out[string(ym)] = append([]string{}, records...) // copy slice
	}
	return out
}

type GroupedPerClient map[ /* clientId */ string]YearMonthGroup

func NewGroupedPerClient() GroupedPerClient {
	return make(GroupedPerClient)
}

func (g *GroupedPerClient) Add(clientId string, records []Record) {
	// If the client's group doesn't exist yet, initialize it
	group, ok := (*g)[clientId]
	if !ok {
		group = NewYearMonthGroup()
	}

	// Use pointer receiver so AddBatch modifies the existing group
	group.AddBatch(records)

	// Store it back
	(*g)[clientId] = group
}

func (g *GroupedPerClient) Get(clientId string) YearMonthGroup {
	if group, ok := (*g)[clientId]; ok {
		return group
	}
	// return an empty group (safe to use immediately)
	return NewYearMonthGroup()
}

func (g *GroupedPerClient) Delete(clientId string) {
	delete(*g, clientId)
	runtime.GC()
}
