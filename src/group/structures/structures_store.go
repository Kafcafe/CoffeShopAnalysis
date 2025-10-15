package structures

import (
	"fmt"
	"strconv"
	"strings"
)

type UserCount int
type StoreGroup map[StoreID]map[UserID]UserCount
type StoreGroupPerClient map[ClientId]StoreGroup

func NewStoreGroup() StoreGroup {
	return make(StoreGroup)
}

func sumCount(existingCount, newCount UserCount) UserCount {
	return existingCount + newCount
}

func (g *StoreGroup) AddBatch(records []Record) {
	for _, record := range records {
		g.Add(record)
	}
}

func (g *StoreGroup) Add(record Record) error {
	parsedRecord, err := parseRecordForSemester(record)
	if err != nil {

		return err
	}

	_, exists := (*g)[parsedRecord.StoreID]
	if !exists {
		(*g)[parsedRecord.StoreID] = make(map[UserID]UserCount)
	}

	if parsedRecord.UserID == "" {
		return nil
	}

	newCount := UserCount(1)
	existingCount, exists := (*g)[parsedRecord.StoreID][parsedRecord.UserID]
	if exists {
		(*g)[parsedRecord.StoreID][parsedRecord.UserID] = sumCount(existingCount, newCount)
	} else {
		(*g)[parsedRecord.StoreID][parsedRecord.UserID] = newCount
	}

	return nil
}

/*
	{
		"storeId1": ["userId1,3", "userId2,6"],
		"storeId2": ["userId1,5", "userId4,5"],
	}
*/
func (g StoreGroup) ToMapString() map[string][]string {
	out := make(map[string][]string, len(g))

	for storeId, users := range g {
		usersPerStore := []string{}

		for userId, count := range users {
			userString := fmt.Sprintf("%s,%d", userId, count)
			usersPerStore = append(usersPerStore, userString)
		}

		out[string(storeId)] = usersPerStore
	}

	return out
}

func (g *StoreGroup) Merge(other StoreGroup) {
	for storeId, users := range other {

		if _, exists := (*g)[storeId]; !exists {
			(*g)[storeId] = make(map[UserID]UserCount)
		}

		for userId, count := range users {
			existing, exists := (*g)[storeId][userId]

			if exists {
				(*g)[storeId][userId] = sumCount(existing, count)
			} else {
				(*g)[storeId][userId] = count
			}
		}
	}
}

////////////////////////////////////////////
////////////////////////////////////////////
////////////////////////////////////////////

func NewStoreGroupFromMapString(m map[string][]string) StoreGroup {
	g := make(StoreGroup)

	for storeStr, userStrs := range m {
		storeId := StoreID(storeStr)
		g[storeId] = make(map[UserID]UserCount)

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

			g[storeId][userId] = UserCount(count)
		}
	}
	return g
}

func (g StoreGroupPerClient) ToMapString() map[string]map[string][]string {
	out := make(map[string]map[string][]string, len(g))

	for clientId, storeGroup := range g {
		out[string(clientId)] = storeGroup.ToMapString()
	}

	return out
}

func NewStoreGroupPerClient() StoreGroupPerClient {
	return make(StoreGroupPerClient)
}

func (g *StoreGroupPerClient) Add(clientId string, records []Record) {
	// If the client's group doesn't exist yet, initialize it
	group, ok := (*g)[clientId]
	if !ok {
		group = NewStoreGroup()
	}

	// Use pointer receiver so AddBatch modifies the existing group
	group.AddBatch(records)

	// Store it back
	(*g)[clientId] = group
}

func (g *StoreGroupPerClient) Get(clientId string) StoreGroup {
	if group, ok := (*g)[clientId]; ok {
		return group
	}
	// return an empty group (safe to use immediately)
	return NewStoreGroup()
}

func (g *StoreGroupPerClient) Delete(clientId string) {
	delete(*g, clientId)
}
