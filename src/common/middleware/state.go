package middleware

import (
	"encoding/json"
)

type WorkerStateDTO struct {
	ClientStats *ClientStats
	Data        []string
}

func NewWorkerState(clientStats *ClientStats, data []string) WorkerStateDTO {
	return WorkerStateDTO{
		ClientStats: clientStats,
		Data:        data,
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
