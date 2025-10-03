package join

import (
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

		newFields := append([]string{}, fields...)
		newFields[leftIndex] = itemName
		joinedItems = append(joinedItems, strings.Join(newFields, ","))
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

func UpdatedSideTableWithUsers(sideTable []string, payload []string) []string {
	// Index rápido para buscar userId → birthdate en el payload
	payloadMap := make(map[string]string, len(payload))
	for _, user := range payload {
		parts := strings.SplitN(user, ",", 2)
		if len(parts) == 2 {
			userId, birthdate := parts[0], parts[1]
			payloadMap[userId] = birthdate
		}
	}

	// Recorremos sideTable directamente y reemplazamos en orden
	updated := make([]string, len(sideTable))
	for i, entry := range sideTable {
		parts := strings.SplitN(entry, ",", 2)
		if len(parts) != 2 {
			updated[i] = entry
			continue
		}
		store, second := parts[0], parts[1]

		// si "second" es un userId y aparece en el payload, lo reemplazamos
		if birthdate, ok := payloadMap[second]; ok && !strings.Contains(second, "-") {
			updated[i] = store + "," + birthdate
		} else {
			updated[i] = entry
		}
	}

	return updated
}
