package group

import (
	"fmt"
	"strconv"
	"strings"
)

type YearMonth string
type Record = string
type ClientId = string

type Item struct {
	TotalQuantity int
	TotalProfit   float64
}

type ItemID string

type YearMonthSum struct {
	yearMonth YearMonth
	items     map[ItemID]Item
}

type GroupedPerClient map[ClientId]YearMonthGroup

type YearMonthGroup map[YearMonth]map[ItemID]Item

func NewYearMonthGroup() YearMonthGroup {
	return make(YearMonthGroup)
}

func (g *YearMonthGroup) AddBatch(records []Record) {
	for _, record := range records {
		g.Add(record)
	}
}

func sumItems(item1, item2 Item) Item {
	return Item{
		TotalQuantity: item1.TotalQuantity + item2.TotalQuantity,
		TotalProfit:   item1.TotalProfit + item2.TotalProfit,
	}
}

func (g *YearMonthGroup) Add(record Record) error {
	parsedRecord, err := parseRecord(record)
	if err != nil {
		return err
	}

	_, exists := (*g)[parsedRecord.yearMonth]
	if !exists {
		(*g)[parsedRecord.yearMonth] = make(map[ItemID]Item)
	}

	item := Item{
		TotalQuantity: parsedRecord.Quantity,
		TotalProfit:   parsedRecord.Profit,
	}

	existingItem, exists := (*g)[parsedRecord.yearMonth][parsedRecord.ItemID]
	if exists {
		(*g)[parsedRecord.yearMonth][parsedRecord.ItemID] = sumItems(existingItem, item)
	} else {
		(*g)[parsedRecord.yearMonth][parsedRecord.ItemID] = item
	}

	return nil
}

//map[YearMonth]map[ItemID]Item

// {
// 	"2025-01": {
// 		"itemId1": {
// 			5, "$10"
// 		},
// 		"itemId2": {
// 			5, "$10"
// 		}
// 	},
// 	"2025-02": {
// 		"itemId1": {
// 			5, "$10"
// 		},
// 		"itemId2": {
// 			5, "$10"
// 		}
// 	}
// }

// {
// 	"2025-01": [
// 		"itemId1", 5, "$10"
// 		"itemId2": 5, "$10"
// 	],
// 	"2025-02": {
// 		"itemId1": {
// 			"total": 5,
// 			"profit": "$10",
// 		},
// 		"itemId2": {
// 			5, "$10"
// 		}
// 	}
// }

func (g YearMonthGroup) ToMapString() map[string][]string {
	out := make(map[string][]string, len(g))

	for ym, yearMonthItems := range g {
		itemsPerYearMonth := []string{}

		for itemId, item := range yearMonthItems {
			itemString := fmt.Sprintf("%s,%d,%f", itemId, item.TotalQuantity, item.TotalProfit)
			itemsPerYearMonth = append(itemsPerYearMonth, itemString)
		}

		out[string(ym)] = itemsPerYearMonth
	}

	return out
}

func NewYearMonthGroupFromMapString(m map[string][]string) YearMonthGroup {
	g := make(YearMonthGroup)

	for ymStr, itemStrs := range m {
		ym := YearMonth(ymStr)
		g[ym] = make(map[ItemID]Item)

		for _, itemStr := range itemStrs {
			parts := strings.Split(itemStr, ",")
			if len(parts) != 3 {
				continue
			}

			itemId := ItemID(parts[0])
			quantity, err := strconv.Atoi(parts[1])
			if err != nil {
				continue
			}

			profit, err := strconv.ParseFloat(parts[2], 64)
			if err != nil {
				continue
			}

			g[ym][itemId] = Item{TotalQuantity: quantity, TotalProfit: profit}
		}
	}
	return g
}

// Merge merges the items from another YearMonthGroup into this one.
func (g *YearMonthGroup) Merge(other YearMonthGroup) {
	for ym, items := range other {

		if _, exists := (*g)[ym]; !exists {
			(*g)[ym] = make(map[ItemID]Item)
		}

		for itemId, item := range items {
			existing, exists := (*g)[ym][itemId]

			if exists {
				(*g)[ym][itemId] = sumItems(existing, item)
			} else {
				(*g)[ym][itemId] = item
			}
		}
	}
}

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
}
