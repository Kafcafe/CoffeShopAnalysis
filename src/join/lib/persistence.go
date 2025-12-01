package join

import (
	"encoding/json"
	"fmt"
)

func (j *JoinGenericWorker) Dump(clientStats *ClientStatsJoin, clientId string) error {

	data, err := clientStats.MarshalJSON()
	if err != nil {
		return fmt.Errorf("error serializing client stats: %v", err)
	}

	if err := j.atomicWritter.WriteLine(string(data), ".json", []string{clientId, "ClientStatsJoin"}); err != nil {
		return fmt.Errorf("error writing client stats to file for client %s: %v", clientId, err)
	}

	return nil
}

func (j *JoinGenericWorker) Recover() error {
	data, err := j.atomicWritter.Recover()

	if err != nil {
		return fmt.Errorf("error recovering client stats: %v", err)
	}

	for clientId, line := range data {
		var dto clientStatsJoinDTO
		if err := json.Unmarshal([]byte(line.GetData()[0]), &dto); err != nil {
			return fmt.Errorf("error deserializing client stats: %v", err)
		}

		clientStats := &ClientStatsJoin{}
		clientStats.fromDTO(dto)

		j.mutex.Lock()
		j.clientsStats[clientId] = clientStats

		// Restore the worker's maps from the recovered data
		j.sideTable[clientId] = make([]string, len(clientStats.sideTable))
		copy(j.sideTable[clientId], clientStats.sideTable)

		j.mainTable[clientId] = make([]string, len(clientStats.mainTable))
		copy(j.mainTable[clientId], clientStats.mainTable)

		// If side table is ready, initialize the channel
		if clientStats.sideTableIsReady {
			if _, exists := j.sideTableReceived[clientId]; !exists {
				j.sideTableReceived[clientId] = make(chan int, SINGLE_ITEM_BUFFER_LEN)
			}
		}

		j.log.Infof("Resurecting client: %v", clientId)
		j.log.Info(clientStats.sideTable)
		j.log.Info(clientStats.mainTable)
		j.log.Infof("Side table is ready: %t", clientStats.sideTableIsReady)
		j.log.Infof("Resurrected tables for client %s: mainTable=%d, sideTable=%d", clientId, len(clientStats.mainTable), len(clientStats.sideTable))
		j.log.Infof("Worker maps synchronized - mainTable=%d, sideTable=%d", len(j.mainTable[clientId]), len(j.sideTable[clientId]))
		j.mutex.Unlock()
	}

	return nil
}
