package group

import (
	"common/middleware"
	"fmt"
)

func (g *GroupByGenericWorker) dumpData(msg *middleware.Message, hash string, dataType DataType) error {
	if g.conf.ofType == GROUP_TYPE_TOPK {
		metadata := []string{msg.ClientId, hash, dataType}
		if err := g.atomicWritter.WriteLines(msg.Payload, metadata); err != nil {
			return fmt.Errorf("error writing grouped data to file for client %s: %v", msg.ClientId, err)
		}
	} else {
		data := g.group.ToFullStringList(msg.ClientId)
		stateJson := middleware.NewWorkerState(g.clientsStats[msg.ClientId], data).ToJson()
		if stateJson == "" {
			return fmt.Errorf("error serializing state to JSON for client %s", msg.ClientId)
		}
		if err := g.atomicWritter.WriteLine(stateJson, ".json", []string{msg.ClientId, "WorkerState"}); err != nil {
			return fmt.Errorf("error writing cache to file for client %s: %v", msg.ClientId, err)
		}
	}
	return nil
}

func (g *GroupByGenericWorker) recover() {

	data, err := g.atomicWritter.Recover()

	if err != nil {
		g.log.Errorf("Error during recovery: %v", err)
		return
	}

	for clientId, data := range data {
		if g.conf.ofType == GROUP_TYPE_TOPK {
			g.log.Warningf("Recovering data (%d) for client %s of datatype: %s", data.GetCount(), clientId, data.GetDataType())
			g.group.Add(clientId, data.GetData())
			g.getClientStats(clientId).SetCount(data.GetDataType(), data.GetCount())
		} else { // is worker state
			g.log.Warningf("Recovering state for client %s", clientId)
			jsonStr := data.GetData()[0]
			state, err := middleware.NewWorkerStateFromJson(jsonStr)
			if err != nil {
				g.log.Errorf("Error deserializing state for client %s: %v\n%s", clientId, err, jsonStr)
				continue
			}
			g.clientsStats[clientId] = state.ClientStats
			g.group.AddFullStringList(clientId, state.Data)
		}
	}
}
