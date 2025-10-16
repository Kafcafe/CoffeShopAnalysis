package structures

import (
	"fmt"
	"strconv"
	"strings"
)

type UserCount int
type StoreGroup map[StoreID]map[UserID]UserCount

type TopKStoreGroup struct {
	k     int
	group StoreGroup
}

func NewTopKStoreGroup(k int) TopKStoreGroup {
	return TopKStoreGroup{
		k:     k,
		group: make(StoreGroup),
	}
}

func sumCount(existingCount, newCount UserCount) UserCount {
	return existingCount + newCount
}

func (g TopKStoreGroup) AddBatch(records []Record) {
	for _, record := range records {
		g.add(record)
	}
}

func (g *TopKStoreGroup) add(record Record) error {
	parsedRecord, err := parseRecordForSemester(record)
	if err != nil {

		return err
	}

	_, exists := g.group[parsedRecord.StoreID]
	if !exists {
		g.group[parsedRecord.StoreID] = make(map[UserID]UserCount)
	}

	if parsedRecord.UserID == "" {
		return nil
	}

	newCount := UserCount(1)
	existingCount, exists := g.group[parsedRecord.StoreID][parsedRecord.UserID]
	if exists {
		g.group[parsedRecord.StoreID][parsedRecord.UserID] = sumCount(existingCount, newCount)
	} else {
		g.group[parsedRecord.StoreID][parsedRecord.UserID] = newCount
	}

	return nil
}

/*
	{
		"storeId1": ["userId1,3", "userId2,6"],
		"storeId2": ["userId1,5", "userId4,5"],
	}
*/
func (g TopKStoreGroup) ToMapString() map[string][]string {
	out := make(map[string][]string, len(g.group))

	for storeId, users := range g.group {
		usersPerStore := []string{}

		for userId, count := range users {
			userString := fmt.Sprintf("%s,%d", userId, count)
			usersPerStore = append(usersPerStore, userString)
		}

		out[string(storeId)] = usersPerStore
	}

	return out
}

func (g TopKStoreGroup) Merge(other AllowedGroup) {
	otherTyped, ok := other.(TopKStoreGroup)
	if !ok {
		return
	}
	for storeId, users := range otherTyped.group {

		if _, exists := g.group[storeId]; !exists {
			g.group[storeId] = make(map[UserID]UserCount)
		}

		for userId, count := range users {
			existing, exists := g.group[storeId][userId]

			if exists {
				g.group[storeId][userId] = sumCount(existing, count)
			} else {
				g.group[storeId][userId] = count
			}
		}
	}
}

////////////////////////////////////////////
////////////////////////////////////////////
////////////////////////////////////////////

func (g TopKStoreGroup) FromMapString(m map[string][]string) AllowedGroup {
	for storeStr, userStrs := range m {
		storeId := StoreID(storeStr)
		g.group[storeId] = make(map[UserID]UserCount)

		for _, userStr := range userStrs {
			parts := strings.Split(userStr, ",")
			if len(parts) != 2 {
				continue
			}

			userId := UserID(parts[0])
			count, err := strconv.Atoi(parts[1])
			if err != nil {
				continue
			}

			g.group[storeId][userId] = UserCount(count)
		}
	}
	return g
}

func (g TopKStoreGroup) GetMessageToSend() []map[string][]string {
	messages := make([]map[string][]string, 0, 1)
	result := make(map[string][]string)

	for storeId, users := range g.group {
		if len(users) == 0 {
			continue
		}
		toper := NewToper(g.k, CmpTransactions)
		for userID, value := range users {
			userId := string(userID)
			if userId == "" {
				continue
			}
			count := int(value)
			if count <= 0 {
				continue
			}
			registry := NewTopKRegister(string(storeId), userId, count)
			toper.Add(registry)
		}
		topKUsers := toper.GetTopK()
		result[string(storeId)] = make([]string, 0, len(topKUsers))
		for _, user := range topKUsers {
			result[string(storeId)] = append(result[string(storeId)], user.String())
		}
	}
	messages = append(messages, result)
	return messages
}
