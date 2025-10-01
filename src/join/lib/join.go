package join

import (
	"fmt"
	"strings"
)

type Join struct{}

func NewJoiner() *Join {
	return &Join{}
}

// rightTable => In memory table, the one that will turn to a map
//
// leftTable => The one that will be iterated
//
// rightIndex => The index of the field that will replace the left field
//
// rightIdIndex => The index of the field that will be used to match with the left table
//
// leftIndex => The index of the field that will be replaced
//
// Example: JoinByIndex(stores, sales, 1, 0, 2) => In stores, index 1 is the name of the store, index 0 is the id of the store. In sales, index 2 is the id of the store
//
// Result: The sales table will have the name of the store instead of the id of the store
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
