package structures

import (
	"fmt"
	"strconv"
	"strings"
)

type Semester string
type TPV float64
type StoreID string

type SemesterGroupPerClient map[ClientId]SemesterGroup

type SemesterGroup map[Semester]map[StoreID]TPV

func NewSemesterGroup() SemesterGroup {
	return make(SemesterGroup)
}

func (g *SemesterGroup) AddBatch(records []Record) {
	for _, record := range records {
		g.Add(record)
	}
}

func sumTpv(existingTpv TPV, newTpv float64) TPV {
	return existingTpv + TPV(newTpv)
}

func (g *SemesterGroup) Add(record Record) error {
	parsedRecord, err := parseRecordForSemester(record)
	if err != nil {
		return err
	}

	_, exists := (*g)[parsedRecord.Semester]
	if !exists {
		(*g)[parsedRecord.Semester] = make(map[StoreID]TPV)
	}

	newTpv := parsedRecord.FinalAmount
	existingTpv, exists := (*g)[parsedRecord.Semester][parsedRecord.StoreID]
	if exists {
		(*g)[parsedRecord.Semester][parsedRecord.StoreID] = sumTpv(existingTpv, newTpv)
	} else {
		(*g)[parsedRecord.Semester][parsedRecord.StoreID] = TPV(newTpv)
	}

	return nil
}

func (g SemesterGroup) ToMapString() map[string][]string {
	out := make(map[string][]string, len(g))

	for sem, semesterStores := range g {
		storesPerSemester := []string{}

		for storeId, tpv := range semesterStores {
			storeString := fmt.Sprintf("%s,%f", storeId, tpv)
			storesPerSemester = append(storesPerSemester, storeString)
		}

		out[string(sem)] = storesPerSemester
	}

	return out
}

func (g *SemesterGroup) Merge(other SemesterGroup) {
	for sem, stores := range other {

		if _, exists := (*g)[sem]; !exists {
			(*g)[sem] = make(map[StoreID]TPV)
		}

		for storeId, tpv := range stores {
			existing, exists := (*g)[sem][storeId]

			if exists {
				(*g)[sem][storeId] = sumTpv(existing, float64(tpv))
			} else {
				(*g)[sem][storeId] = tpv
			}
		}
	}
}

func NewSemesterGroupFromMapString(m map[string][]string) SemesterGroup {
	g := make(SemesterGroup)

	for semStr, storeStrs := range m {
		sem := Semester(semStr)
		g[sem] = make(map[StoreID]TPV)

		for _, storeStr := range storeStrs {
			parts := strings.Split(storeStr, ",")
			if len(parts) != 2 {
				continue
			}

			storeId := StoreID(parts[0])
			tpv, err := strconv.ParseFloat(parts[1], 64)
			if err != nil {
				continue
			}

			g[sem][storeId] = TPV(tpv)
		}
	}
	return g
}

func NewSemesterGroupPerClient() SemesterGroupPerClient {
	return make(SemesterGroupPerClient)
}

func (g *SemesterGroupPerClient) Add(clientId string, records []Record) {
	// If the client's group doesn't exist yet, initialize it
	group, ok := (*g)[clientId]
	if !ok {
		group = NewSemesterGroup()
	}

	// Use pointer receiver so AddBatch modifies the existing group
	group.AddBatch(records)

	// Store it back
	(*g)[clientId] = group
}

func (g *SemesterGroupPerClient) Get(clientId string) SemesterGroup {
	if group, ok := (*g)[clientId]; ok {
		return group
	}
	// return an empty group (safe to use immediately)
	return NewSemesterGroup()
}

func (g *SemesterGroupPerClient) Delete(clientId string) {
	delete(*g, clientId)
}
