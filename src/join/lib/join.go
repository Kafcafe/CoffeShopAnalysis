package join

import (
	"fmt"
	"strings"
)

type Join struct{}

func NewJoiner() *Join {
	return &Join{}
}

func (j *Join) JoinByIndex(rightTable []string, leftTable []string, rightIndex, rightIdIndex, leftIndex int) []string {
	joinedItems := make([]string, 0)
	if len(rightTable) == 0 || len(leftTable) == 0 {
		return joinedItems
	}

	itemMap := j.generateMapItemsByIndex(rightTable, rightIndex, rightIdIndex)
	fmt.Println(itemMap)
	for _, lItem := range leftTable {
		fields := strings.Split(lItem, ",")
		if len(fields) <= leftIndex {
			continue
		}

		itemId := strings.TrimSpace(fields[leftIndex])
		itemName := itemMap[itemId]
		if itemName == "" {
			continue
		}

		joinedItem := strings.Join(fields[:leftIndex], ",") + "," + itemName + "," + strings.Join(fields[leftIndex+1:], ",")
		joinedItems = append(joinedItems, joinedItem)
	}
	return joinedItems
}

// / stores, 1 => alli esta el nombre de la store, 0 => alli esta el id
func (j *Join) generateMapItemsByIndex(items []string, index, idIndex int) map[string]string {
	mapItems := make(map[string]string)
	for _, item := range items {
		fields := strings.Split(item, ",")

		if len(fields) <= index {
			continue
		}

		id := strings.TrimSpace(fields[idIndex])
		name := strings.TrimSpace(fields[index])
		mapItems[id] = name
	}
	return mapItems
}
