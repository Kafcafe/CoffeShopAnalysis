package middleware

import (
	"encoding/json"
)

type WorkerStateDTO struct {
	ClientStats *ClientStats
	Data        []string
	Cache       *Cache
}

func NewWorkerState(clientStats *ClientStats, data []string, cache *Cache) WorkerStateDTO {
	return WorkerStateDTO{
		ClientStats: clientStats,
		Data:        data,
		Cache:       cache,
	}
}

func (state WorkerStateDTO) ToJson() string {
	result, err := json.Marshal(state)
	if err != nil {
		return ""
	}
	return string(result)
}

func NewWorkerStateFromJson(jsonStr string) (*WorkerStateDTO, error) {
	bytes := []byte(jsonStr)
	var state WorkerStateDTO
	if err := json.Unmarshal(bytes, &state); err != nil {
		return nil, err
	}
	return &state, nil
}
