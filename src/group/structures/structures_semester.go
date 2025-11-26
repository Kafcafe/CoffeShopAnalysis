package structures

import (
	"fmt"
	"strconv"
	"strings"
)

type Semester string
type TPV float64
type StoreID = string

type SemesterGroup map[Semester]map[StoreID]TPV

func NewSemesterGroup() SemesterGroup {
	return make(SemesterGroup)
}

func (g SemesterGroup) AddBatch(records []Record) {
	for _, record := range records {
		g.add(record)
	}
}

func sumTpv(existingTpv TPV, newTpv float64) TPV {
	return existingTpv + TPV(newTpv)
}

func (g *SemesterGroup) add(record Record) error {
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
			storeString := fmt.Sprintf("%s,%.2f", storeId, tpv)
			storesPerSemester = append(storesPerSemester, storeString)
		}

		out[string(sem)] = storesPerSemester
	}

	return out
}

func (g SemesterGroup) AddMapString(data map[string][]string) {
	other := NewSemesterGroupFromMap(data)
	g.merge(other)
}

func (g SemesterGroup) merge(other SemesterGroup) {
	// Merge the two maps
	for sem, stores := range other {

		if _, exists := g[sem]; !exists {
			g[sem] = make(map[StoreID]TPV)
		}

		for storeId, tpv := range stores {
			existing, exists := g[sem][storeId]

			if exists {
				g[sem][storeId] = sumTpv(existing, float64(tpv))
			} else {
				g[sem][storeId] = tpv
			}
		}
	}
}

func NewSemesterGroupFromMap(data map[string][]string) SemesterGroup {
	g := NewSemesterGroup()
	for semStr, storeStrs := range data {
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

func (g SemesterGroup) GetMessageToSend() []map[string][]string {
	messages := make([]map[string][]string, 0, 1)
	messages = append(messages, g.ToMapString())
	return messages
}

func (g SemesterGroup) ToFullStringList() []string {
	out := []string{}
	mapInfo := g.ToMapString()
	for sem, storeStrs := range mapInfo {
		for _, storeStr := range storeStrs {
			line := fmt.Sprintf("%s,%s", sem, storeStr)
			out = append(out, line)
		}
	}
	return out
}

func (g SemesterGroup) Recover(data []string) error {
	for _, record := range data {
		err := g.add(Record(record))
		if err != nil {
			return err
		}
	}
	return nil
}
