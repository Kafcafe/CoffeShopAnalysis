package structures

import (
	"fmt"
	"strconv"
	"strings"
)

type UserCount = int
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

func (g TopKStoreGroup) AddBatch(records []Record) {
	for _, record := range records {
		g.add(record)
	}
}

func (g *TopKStoreGroup) add(record Record) error {
	fields := strings.Split(record, ",")
	storeID := fields[1]
	userID := fields[2]

	if userID == "" {
		return nil
	}

	if _, exists := g.group[storeID]; !exists {
		g.group[storeID] = make(map[UserID]UserCount)
	}

	existingCount, exists := g.group[storeID][userID]
	if exists {
		g.group[storeID][userID] = existingCount + 1
	} else {
		g.group[storeID][userID] = 1
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

func (g TopKStoreGroup) AddMapString(data map[string][]string) {
	other := NewTopKStoreGroupFromMap(data, g.k)
	g.merge(other)
}

func (g TopKStoreGroup) merge(other TopKStoreGroup) {
	for otherStoreId, otherUsers := range other.group {

		if _, exists := g.group[otherStoreId]; !exists {
			g.group[otherStoreId] = make(map[UserID]UserCount)
		}

		for otherUserId, otherUserCount := range otherUsers {
			userCount, userExists := g.group[otherStoreId][otherUserId]

			if userExists {
				g.group[otherStoreId][otherUserId] = userCount + otherUserCount
			} else {
				g.group[otherStoreId][otherUserId] = otherUserCount
			}
		}
	}
}

func (g TopKStoreGroup) ToFullStringList() []string {
	out := []string{}
	maps := g.ToMapString()
	for storeId, users := range maps {
		for _, userStr := range users {
			out = append(out, fmt.Sprintf("%s,%s", storeId, userStr))
		}
	}
	return out
}

////////////////////////////////////////////
////////////////////////////////////////////
////////////////////////////////////////////

func NewTopKStoreGroupFromMap(m map[string][]string, k int) TopKStoreGroup {
	g := NewTopKStoreGroup(k)
	for storeId, userStrs := range m {
		g.group[storeId] = make(map[UserID]UserCount)

		for _, userStr := range userStrs {
			parts := strings.Split(userStr, ",")
			userId := UserID(parts[0])
			count, err := strconv.Atoi(parts[1])
			if err != nil {
				count = 0
			}
			if existingCount, exists := g.group[storeId][userId]; exists {
				g.group[storeId][userId] = count + existingCount
			} else {
				g.group[storeId][userId] = count
			}
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
		topKUsers := toper.GetTopKWithKeys()
		result[string(storeId)] = make([]string, 0, len(topKUsers))
		for _, userCountPair := range topKUsers {
			countResult := userCountPair.Value
			userResult := userCountPair.Key
			result[string(storeId)] = append(result[string(storeId)], fmt.Sprintf("%s,%d", userResult, countResult))
		}
	}
	messages = append(messages, result)
	return messages
}

func (g TopKStoreGroup) Recover(data []string) error {
	for _, record := range data {
		err := g.add(Record(record))
		if err != nil {
			return err
		}
	}
	return nil
}
