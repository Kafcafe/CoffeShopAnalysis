package join

import (
	"common/middleware"
	"encoding/json"
	"sync"
)

type ClientStatsJoin struct {
	mainTable        []string
	sideTable        []string
	stats            *middleware.ClientStats
	sideTableIsReady bool
	mtx              sync.Mutex
}

func NewClientStatsJoin(cacheCapacity int) *ClientStatsJoin {
	return &ClientStatsJoin{
		mainTable: []string{},
		sideTable: []string{},
		stats:     middleware.NewClientStats(cacheCapacity),
	}
}

func (cs *ClientStatsJoin) Add(dataType middleware.DataType, messageId string, processed, emitted bool, table string, payload []string) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.stats.Add(dataType, messageId, processed, emitted)
	switch table {
	case "main":
		cs.mainTable = append(cs.mainTable, payload...)
	case "side":
		cs.sideTable = append(cs.sideTable, payload...)
	case "users":
		cs.sideTable = payload
	}
}

func (cs *ClientStatsJoin) GetStats(dataType middleware.DataType) (processed, emitted int, mainTable []string, sideTable []string) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	processed, emitted = cs.stats.GetStats(dataType)
	return processed, emitted, cs.mainTable, cs.sideTable
}

func (cs *ClientStatsJoin) Clear(dataType middleware.DataType) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.stats.Clear(dataType)
	cs.mainTable = []string{}
	cs.sideTable = []string{}
}

func (cs *ClientStatsJoin) WasMessageProcessed(messageId string) bool {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	return cs.stats.WasMessageProcessed(messageId)
}

func (cs *ClientStatsJoin) MarkSideTableAsReady() {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.sideTableIsReady = true
}

func (cs *ClientStatsJoin) Remove(dataType middleware.DataType, processed, emitted int) {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.stats.Remove(dataType, processed, emitted)
}

type clientStatsJoinDTO struct {
	MainTable        []string                `json:"main_table"`
	SideTable        []string                `json:"side_table"`
	SideTableIsReady bool                    `json:"side_table_is_ready"`
	Stats            *middleware.ClientStats `json:"stats"`
}

func (cs *ClientStatsJoin) toDTO() clientStatsJoinDTO {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()

	return clientStatsJoinDTO{
		MainTable:        cs.mainTable,
		SideTable:        cs.sideTable,
		SideTableIsReady: cs.sideTableIsReady,
		Stats:            cs.stats,
	}
}

func (cs *ClientStatsJoin) MarshalJSON() ([]byte, error) {
	dto := cs.toDTO()
	return json.Marshal(dto)
}

func (cs *ClientStatsJoin) fromDTO(dto clientStatsJoinDTO) {
	cs.mainTable = dto.MainTable
	cs.sideTable = dto.SideTable
	cs.stats = dto.Stats
	cs.sideTableIsReady = dto.SideTableIsReady
}

func (cs *ClientStatsJoin) UnmarshalJSON(data []byte) error {
	var dto clientStatsJoinDTO
	if err := json.Unmarshal(data, &dto); err != nil {
		return err
	}
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	cs.fromDTO(dto)
	return nil
}
