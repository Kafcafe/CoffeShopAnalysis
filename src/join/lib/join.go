package join

import (
	"strings"
)

type Join struct{}

func NewJoiner() *Join {
	return &Join{}
}

func (j *Join) JoinItemNameById(items []string, transactionItems []string) []string {
	joinedItems := make([]string, 0)
	if len(items) == 0 || len(transactionItems) == 0 {
		return joinedItems
	}
	itemMap := j.generateMapItems(items)
	for _, tItem := range transactionItems {
		fields := strings.Split(tItem, ",")
		if len(fields) < 2 {
			continue
		}

		itemId := strings.TrimSpace(fields[1])
		itemName := itemMap[itemId]
		if itemName == "" {
			continue
		}

		joinedItem := fields[0] + "," + itemName + "," + strings.Join(fields[2:], ",")
		joinedItems = append(joinedItems, joinedItem)
	}
	return joinedItems
}

func (j *Join) generateMapItems(items []string) map[string]string {
	mapItems := make(map[string]string)
	for _, item := range items {
		fields := strings.Split(item, ",")

		if len(fields) < 2 {
			continue
		}

		id := strings.TrimSpace(fields[0])
		name := strings.TrimSpace(fields[1])
		mapItems[id] = name
	}
	return mapItems
}
