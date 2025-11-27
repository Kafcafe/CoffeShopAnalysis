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
		return nil
	}

	toSave := g.group.ToFullStringList(msg.ClientId)
	state := middleware.NewWorkerState(g.clientsStats[msg.ClientId], toSave)
	jsonStr := state.ToJson()
	if jsonStr == "" {
		return fmt.Errorf("error serializing state to JSON for client %s", msg.ClientId)
	}
	if err := g.atomicWritter.WriteLine(jsonStr, ".json", []string{msg.ClientId, "WorkerState"}); err != nil {
		return fmt.Errorf("error writing cache to file for client %s: %v", msg.ClientId, err)
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
		g.log.Infof("Recovering data for client %s", clientId)
		if g.conf.ofType == GROUP_TYPE_TOPK {
			g.group.Add(clientId, data.GetData())
			g.getClientStats(clientId).SetCount(data.GetDataType(), data.GetCount())
		} else {
			jsonStr := data.GetData()[0]
			state, err := middleware.NewWorkerStateFromJson(jsonStr)
			if err != nil {
				g.log.Errorf("Error deserializing state for client %s: %v", clientId, err)
				continue
			}
			g.clientsStats[clientId] = state.ClientStats
			g.group.AddFullStringList(clientId, state.Data)
		}
	}

}
