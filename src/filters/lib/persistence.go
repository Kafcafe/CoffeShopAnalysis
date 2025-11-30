package filters

import (
	"common/middleware"
	"fmt"
)

func (f *FilterGenericWorker) saveWorkerState(clientId string) error {
	stateJson := middleware.NewWorkerState(f.clientsStats[clientId], nil).ToJson()
	if stateJson == "" {
		return fmt.Errorf("error serializing state to JSON for client %s", clientId)
	}
	if err := f.atomicWritter.WriteLine(stateJson, ".json", []string{clientId, "WorkerState"}); err != nil {
		return fmt.Errorf("error writing cache to file for client %s: %v", clientId, err)
	}
	return nil
}

func (f *FilterGenericWorker) recover() {

	data, err := f.atomicWritter.Recover()

	if err != nil {
		f.log.Errorf("Error during recovery: %v", err)
		return
	}

	for clientId, data := range data {
		// is worker state
		f.log.Warningf("Recovering state for client %s", clientId)
		jsonStr := data.GetData()[0]
		state, err := middleware.NewWorkerStateFromJson(jsonStr)
		if err != nil {
			f.log.Errorf("Error deserializing state for client %s: %v\n%s", clientId, err, jsonStr)
			continue
		}
		f.clientsStats[clientId] = state.ClientStats
	}
}
