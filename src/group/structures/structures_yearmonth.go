package structures

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

type YearMonthGroup map[YearMonth]map[ItemID]Item

func NewYearMonthGroup() YearMonthGroup {
	return make(YearMonthGroup)
}

func (g YearMonthGroup) AddBatch(records []Record) {
	for _, record := range records {
		g.add(record)
	}
}

func sumItems(item1, item2 Item) Item {
	return Item{
		TotalQuantity: item1.TotalQuantity + item2.TotalQuantity,
		TotalProfit:   item1.TotalProfit + item2.TotalProfit,
	}
}

func (g YearMonthGroup) add(record Record) error {
	parsedRecord, err := parseRecordForYearMonth(record)
	if err != nil {
		return err
	}

	_, exists := g[parsedRecord.yearMonth]
	if !exists {
		g[parsedRecord.yearMonth] = make(map[ItemID]Item)
	}

	item := Item{
		TotalQuantity: parsedRecord.Quantity,
		TotalProfit:   parsedRecord.Profit,
	}

	existingItem, exists := g[parsedRecord.yearMonth][parsedRecord.ItemID]
	if exists {
		g[parsedRecord.yearMonth][parsedRecord.ItemID] = sumItems(existingItem, item)
	} else {
		g[parsedRecord.yearMonth][parsedRecord.ItemID] = item
	}

	return nil
}

// FROM:
//
//	{
//		"2025-01": {
//			"itemId1": {
//				5, "$10"
//			},
//			"itemId2": {
//				5, "$10"
//			}
//		},
//		"2025-02": {
//			"itemId1": {
//				5, "$10"
//			},
//			"itemId2": {
//				5, "$10"
//			}
//		}
//	}
//
// TO:
//
//	{
//		"2025-01": [
//			"itemId1", 5, "$10"
//			"itemId2": 5, "$10"
//		],
//		"2025-02": {
//			"itemId1": {
//				"total": 5,
//				"profit": "$10",
//			},
//			"itemId2": {
//				5, "$10"
//			}
//		}
//	}
func (g YearMonthGroup) ToMapString() map[string][]string {
	out := make(map[string][]string, len(g))

	for ym, yearMonthItems := range g {
		itemsPerYearMonth := []string{}

		for itemId, item := range yearMonthItems {
			itemString := fmt.Sprintf("%s,%d,%.2f", itemId, item.TotalQuantity, item.TotalProfit)
			itemsPerYearMonth = append(itemsPerYearMonth, itemString)
		}

		out[string(ym)] = itemsPerYearMonth
	}

	return out
}

func (g YearMonthGroup) AddMapString(data map[string][]string) {
	other := NewYearMonthGroupFromMap(data)
	g.merge(other)
}

func NewYearMonthGroupFromMap(data map[string][]string) YearMonthGroup {
	g := NewYearMonthGroup()
	for ymStr, itemStrs := range data {
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
func (g YearMonthGroup) merge(other YearMonthGroup) {
	for ym, items := range other {

		if _, exists := g[ym]; !exists {
			g[ym] = make(map[ItemID]Item)
		}

		for itemId, item := range items {
			existing, exists := g[ym][itemId]

			if exists {
				g[ym][itemId] = sumItems(existing, item)
			} else {
				g[ym][itemId] = item
			}
		}
	}
}

func (g YearMonthGroup) GetMessageToSend() []map[string][]string {
	messages := make([]map[string][]string, 0, 2)
	messages = append(messages, g.getTopProfit().ToMapString())
	messages = append(messages, g.getBestSeller().ToMapString())
	return messages
}

func (g YearMonthGroup) getTopProfit() YearMonthGroup {
	result := NewYearMonthGroup()
	for ym, items := range g {
		if len(items) == 0 {
			continue
		}
		var maxProfit float64 = -1e9
		var bestItemID ItemID
		var bestItem Item
		for itemID, item := range items {
			if item.TotalProfit > maxProfit {
				maxProfit = item.TotalProfit
				bestItemID = itemID
				bestItem = item
			}
		}
		if maxProfit > -1e9 {
			bestItem.TotalQuantity = 0
			result[ym] = make(map[ItemID]Item)
			result[ym][bestItemID] = bestItem
		}
	}
	return result
}

func (g YearMonthGroup) getBestSeller() YearMonthGroup {
	result := NewYearMonthGroup()
	for ym, items := range g {
		if len(items) == 0 {
			continue
		}
		var maxQuantity int = -1
		var bestItemID ItemID
		var bestItem Item
		for itemID, item := range items {
			if item.TotalQuantity > maxQuantity {
				maxQuantity = item.TotalQuantity
				bestItemID = itemID
				bestItem = item
			}
		}
		if maxQuantity > -1 {
			bestItem.TotalProfit = 0
			result[ym] = make(map[ItemID]Item)
			result[ym][bestItemID] = bestItem
		}
	}
	return result
}

func (g YearMonthGroup) ToFullStringList() []string {
	out := []string{}
	mapInfo := g.ToMapString()
	for ym, itemStrs := range mapInfo {
		for _, itemStr := range itemStrs {
			line := fmt.Sprintf("%s,%s", ym, itemStr)
			out = append(out, line)
		}
	}
	return out
}
